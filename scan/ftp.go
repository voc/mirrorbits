// Copyright (c) 2014-2015 Ludovic Fauvet
// Licensed under the MIT license

package scan

import (
	"fmt"
	"github.com/etix/goftp"
	"github.com/etix/mirrorbits/utils"
	"github.com/garyburd/redigo/redis"
	"net/url"
	"os"
	"strings"
	"time"
)

type FTPScanner struct {
	scan *scan
}

func (f *FTPScanner) Scan(scanurl, identifier string, conn redis.Conn, stop chan bool) error {
	if !strings.HasPrefix(scanurl, "ftp://") {
		return fmt.Errorf("%s does not start with ftp://", scanurl)
	}

	ftpurl, err := url.Parse(scanurl)
	if err != nil {
		return err
	}

	host := ftpurl.Host
	if !strings.Contains(host, ":") {
		host += ":21"
	}

	if utils.IsStopped(stop) {
		return ScanAborted
	}

	c, err := ftp.DialTimeout(host, 5*time.Second)
	if err != nil {
		return err
	}
	defer c.Quit()

	username, password := "anonymous", "anonymous"

	if ftpurl.User != nil {
		username = ftpurl.User.Username()
		pass, hasPassword := ftpurl.User.Password()
		if hasPassword {
			password = pass
		}
	}

	err = c.Login(username, password)
	if err != nil {
		return err
	}

	log.Infof("[%s] Requesting file list via ftp...", identifier)

	if _, ok := c.Feature("MLST"); !ok {
		log.Warning("This server does not support the RFC 3659, consider using rsync instead of FTP!")
	}

	var files []*filedata = make([]*filedata, 0, 1000)

	err = c.ChangeDir(ftpurl.Path)
	if err != nil {
		return fmt.Errorf("ftp error %s", err.Error())
	}

	prefixDir, err := c.CurrentDir()
	if err != nil {
		return fmt.Errorf("ftp error %s", err.Error())
	}
	if os.Getenv("DEBUG") != "" {
		_ = prefixDir
		//fmt.Printf("[%s] Current dir: %s\n", identifier, prefixDir)
	}

	files, err = f.walkFtp(c, files, "", stop)
	if err != nil {
		return fmt.Errorf("ftp error %s", err.Error())
	}

	count := 0
	for _, fd := range files {
		if os.Getenv("DEBUG") != "" {
			fmt.Printf("%s\n", fd.path)
		}

		f.scan.ScannerAddFile(*fd)

		count++
	}

	return nil
}

// Walk inside an FTP repository
func (f *FTPScanner) walkFtp(c *ftp.ServerConn, files []*filedata, path string, stop chan bool) ([]*filedata, error) {
	if utils.IsStopped(stop) {
		return nil, ScanAborted
	}

	flist, err := c.List(path)
	if err != nil {
		return nil, err
	}
	for _, e := range flist {
		if e.Type == ftp.EntryTypeFile {
			newf := &filedata{}
			newf.path = "/" + path + "/" + e.Name
			newf.size = int64(e.Size)
			newf.modTime = e.Time
			files = append(files, newf)
		} else if e.Type == ftp.EntryTypeFolder {
			newpath := path
			if len(newpath) > 0 {
				newpath += "/"
			}
			newpath += e.Name
			files, err = f.walkFtp(c, files, newpath, stop)
			if err != nil {
				return files, err
			}
		}
	}
	return files, err
}
