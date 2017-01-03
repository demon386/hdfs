package hdfs

import (
	"log"
	"os"
	"os/exec"
	"path"
	"regexp"
	"strings"
)

type HDFS struct {
	hadoopHome string
	hadoopCmd  string
}

func NewHDFS() HDFS {
	hadoopHome := os.Getenv("HADOOP_HOME")
	if hadoopHome == "" {
		log.Fatal("please set HADOOP_HOME environment variable")
	}
	return HDFS{
		hadoopHome: hadoopHome,
		hadoopCmd:  path.Join(hadoopHome, "bin", "hadoop"),
	}
}

func extractHDFSPrefix(path string) string {
	re, _ := regexp.Compile(`(hdfs:///*.*?)/.*`)
	result := re.FindStringSubmatch(path)
	if len(result) == 2 {
		return result[1]
	} else {
		return ""
	}
}

func (s HDFS) Ls(inputPath string) []string {
	hdfsPrefix := extractHDFSPrefix(inputPath)
	log.Printf("ls: %s\n", inputPath)
	output := s.Exec("fs", "-ls", inputPath)
	outputSplit := strings.Split(output, "\n")
	files := []string{}
	for _, line := range outputSplit {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "-") || line == "" {
			continue
		}
		split := strings.Fields(line)
		fullPath := hdfsPrefix + split[len(split)-1]
		files = append(files, fullPath)
	}
	return files
}

func (s HDFS) Get(remote string, local string) {
	s.Exec("fs", "-get", remote, local)
}

func (s HDFS) Gets(remoteFiles []string, local string) {
	for _, f := range remoteFiles {
		s.Get(f, local)
	}
}

func (s HDFS) Exec(subCmd string, args ...string) string {
	jobConf := []string{}
	fullCmd := []string{
		subCmd,
	}
	for _, args := range [][]string{jobConf, args} {
		fullCmd = append(fullCmd, args...)
	}
	cmd := exec.Command(s.hadoopCmd, fullCmd...)
	stdout, err := cmd.CombinedOutput()
	if err != nil {
		log.Printf("failed with command: %s %s", s.hadoopCmd, strings.Join(fullCmd, " "))
	}
	return string(stdout)
}
