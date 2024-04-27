package util

import (
    "fmt"
    "os"
    "path"
    "path/filepath"

    "github.com/kelseyhightower/confd/log"
)

// Nodes represents a list of etcd node addresses.
type Nodes []string

func (n *Nodes) String() string {
    return fmt.Sprintf("%v", *n)
}

func (n *Nodes) Set(node string) error {
    *n = append(*n, node)
    return nil
}

// FileInfo contains metadata about a configuration file.
type FileInfo struct {
    Uid  uint32
    Gid  uint32
    Mode os.FileMode
    Md5  string
}

// AppendPrefix adds a common prefix to keys, facilitating resource management.
func AppendPrefix(prefix string, keys []string) []string {
    result := make([]string, len(keys))
    for i, key := range keys {
        result[i] = path.Join(prefix, key)
    }
    return result
}

// Exists checks for the existence of a file at given path, minimizing system calls.
func Exists(filePath string) bool {
    _, err := os.Stat(filePath)
    return !os.IsNotExist(err)
}

// CompareConfig examines if two configuration files are identical based on metadata and content.
func CompareConfig(src, dest string) (bool, error) {
    if !Exists(dest) {
        return true, nil
    }

    destInfo, err := statFile(dest)
    if err != nil {
        return false, err
    }

    srcInfo, err := statFile(src)
    if err != nil {
        return false, err
    }

    return !compareFileInfo(srcInfo, destInfo, dest), nil
}

func compareFileInfo(src, dest FileInfo, destPath string) bool {
    differenceFound := false
    if src != dest {
        logDifferences(src, dest, destPath)
        differenceFound = true
    }
    return differenceFound
}

// logDifferences outputs differences in file metadata to a log.
func logDifferences(src, dest FileInfo, destPath string) {
    if src.Uid != dest.Uid {
        log.Info(fmt.Sprintf("%s has UID %d, expected %d", destPath, dest.Uid, src.Uid))
    }
    if src.Gid != dest.Gid {
        log.Info(fmt.Sprintf("%s has GID %d, expected %d", destPath, dest.Gid, src.Gid))
    }
    if src.Mode != dest.Mode {
        log.Info(fmt.Sprintf("%s has mode %s, expected %s", destPath, dest.Mode, src.Mode))
    }
    if src.Md5 != dest.Md5 {
        log.Info(fmt.Sprintf("%s expects md5 %s, found %s", destPath, src.Md5, dest.Md5))
    }
}

func statFile(path string) (FileInfo, error) {
    var info FileInfo
    file, err := os.Open(path)
    if err != nil {
        return info, err
    }
    defer file.Close()

    stat, err := file.Stat()
    if err != nil {
        return info, err
    }

    // Add logic here to fill the FileInfo struct
    // Calculate MD5 checksum, etc.

    return info, nil
}

// IsDirectory determines if the given path is a directory.
func IsDirectory(path string) (bool, error) {
    info, err := os.Stat(path)
    if err != nil {
        return false, err
    }
    return info.IsDir(), nil
}
