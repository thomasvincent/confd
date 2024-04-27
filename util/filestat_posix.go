// +build !windows

package util

import (
    "crypto/sha256"
    "fmt"
    "io"
    "os"
    "syscall"
)

// FileInfo holds the metadata about a file including UID, GID, mode, and SHA-256 checksum.
type FileInfo struct {
    Uid  uint32
    Gid  uint32
    Mode os.FileMode
    Sha256 string
}

// FileStat returns a FileInfo describing the named file, including its SHA-256 checksum.
// If the file does not exist or there is an error processing it, FileStat returns an error
// detailing the issue encountered.
func FileStat(name string) (FileInfo, error) {
    var fi FileInfo
    stat, err := os.Stat(name)
    if err != nil {
        if os.IsNotExist(err) {
            return fi, fmt.Errorf("file not found: %s", name)
        }
        return fi, fmt.Errorf("error accessing file %s: %w", name, err)
    }

    f, err := os.Open(name)
    if err != nil {
        return fi, fmt.Errorf("error opening file %s: %w", name, err)
    }
    defer f.Close()

    sysStat, ok := stat.Sys().(*syscall.Stat_t)
    if !ok {
        return fi, fmt.Errorf("file stats assertion failed for file %s", name)
    }

    fi.Uid = sysStat.Uid
    fi.Gid = sysStat.Gid
    fi.Mode = stat.Mode()

    hash := sha256.New()
    if _, err := io.Copy(hash, f); err != nil {
        return fi, fmt.Errorf("error calculating SHA-256 for file %s: %w", name, err)
    }
    fi.Sha256 = fmt.Sprintf("%x", hash.Sum(nil))

    return fi, nil
}
