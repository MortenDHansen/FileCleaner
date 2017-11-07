package main

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
)

// Configuration for the process run
// Must include
// - a list of files,
// - the directory to look in (absolute or relative)
// - A list of string prefixes to remove (remove all lines beginning with ...)
type Configuration struct {
	Files    []string
	Remove   []string
	FilesDir string
}

func main() {
	fmt.Printf("SqlCleaner v0.1 \n")
	config := configuration()
	fdir, _ := filepath.Abs(config.FilesDir)

	foundFiles, err := ioutil.ReadDir(fdir)
	checkError(err)

	parseFiles := findMatchingFile(foundFiles, config)
	fmt.Println("These files are found for parsing: ")
	fmt.Println(parseFiles)

	numberOfFiles := len(parseFiles)
	var wg sync.WaitGroup
	wg.Add(numberOfFiles)

	for i := 0; i < numberOfFiles; i++ {
		file := parseFiles[i]
		go readCompressedFile(file, config, &wg)
	}
	wg.Wait()
	fmt.Println("\n Files processed...")
}

func findMatchingFile(foundFiles []os.FileInfo, config Configuration) []string {
	var parseFiles []string
	for _, file := range foundFiles {
		for _, name := range config.Files {
			var re = regexp.MustCompile(`(?U)^(` + name + `)(_\d{4}-\d{2}-\d{2})`)
			if len(re.FindString(file.Name())) > 0 {
				parseFiles = append(parseFiles, file.Name())
			}
		}
	}
	return parseFiles
}

func readCompressedFile(filename string, config Configuration, wg *sync.WaitGroup) {

	// Open file
	fmt.Println("\n Parse file: " + config.FilesDir + filename)
	fdir, _ := filepath.Abs(config.FilesDir)
	file, err := os.Open(fdir + "/" + filename)
	checkError(err)
	defer file.Close()

	// read Zipped
	reader, err := gzip.NewReader(file)
	ucr := bufio.NewReader(reader)
	checkError(err)

	// create file for output
	outFile, err := os.Create("parsed_" + filename)
	checkError(err)
	defer outFile.Close()

	// write zipped
	zwriter := gzip.NewWriter(outFile)

	var skipline = false
	var line string
	for {
		line, err = ucr.ReadString('\n')

		if err != nil {
			break
		}

		// remove section
		if strings.HasPrefix(line, "/*!50003 CREATE*/ /*!50017") {
			skipline = true
		}
		if skipline == true && strings.HasPrefix(line, "END */;;") {
			skipline = false
			continue
		}

		if skipline == true {
			continue
		}

		if shouldRemove(line, config.Remove) {
			continue
		}

		zwriter.Write([]byte(line))
	}
	zwriter.Close()

	if err != io.EOF {
		fmt.Printf(" > Failed!: %v\n", err)
	}
	fmt.Println("\n file: parsed_" + filename + " done")
	wg.Done()
}

func readFileWithReadString(filename string, config Configuration) (err error) {

	file, err := os.Open(filename)
	defer file.Close()
	checkError(err)

	reader := bufio.NewReader(file)

	outFile, err := os.Create("parsed_" + filename)
	checkError(err)
	defer outFile.Close()
	var skipline = false
	var line string
	for {
		line, err = reader.ReadString('\n')

		if err != nil {
			break
		}

		// remove section
		if strings.HasPrefix(line, "/*!50003 CREATE*/ /*!50017") {
			skipline = true
		}
		if skipline == true && strings.HasPrefix(line, "END */;;") {
			skipline = false
			continue
		}

		if skipline == true {
			continue
		}

		if shouldRemove(line, config.Remove) {
			continue
		}

		outFile.WriteString(line)
	}

	if err != io.EOF {
		fmt.Printf(" > Failed!: %v\n", err)
	}

	return
}

func shouldRemove(line string, removeList []string) bool {
	for _, removeString := range removeList {
		if strings.HasPrefix(line, removeString) {
			return true
		}
	}
	return false
}

func checkError(err error) {
	if err != nil {
		fmt.Println("\n CRAP, Fuck me... An error occured... \n", err.Error())
	}
}

func configuration() Configuration {
	configfile, _ := os.Open("config.json")
	decoder := json.NewDecoder(configfile)
	configuration := Configuration{}

	err := decoder.Decode(&configuration)
	checkError(err)
	return configuration
}
