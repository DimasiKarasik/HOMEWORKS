package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

type Result struct {
	Level   string
	Message string
}

const (
	logFileName = "app.log"
	totalLines  = 10000
	chunkSize   = 2500 
)

func main() {
	// Шаг 1: Генерируем файл с 10 000 строк
	if err := generateLogFile(logFileName, totalLines); err != nil {
		fmt.Printf("Ошибка создания файла: %v\n", err)
		return
	}
	defer os.Remove(logFileName) 

	fmt.Println("=== ЗАПУСК ТЕСТОВ ПРОИЗВОДИТЕЛЬНОСТИ ===")

	t1 := runSingleWorker(logFileName, 0)
	fmt.Printf("1. Без буфера канала (построчно, 1 воркер):      %v\n", t1)

	t2 := runSingleWorker(logFileName, 1000)
	fmt.Printf("2. С буфером канала 1000 (построчно, 1 воркер):   %v\n", t2)

	t3 := runWithWaitGroup(logFileName, 1000, 4)
	fmt.Printf("3. С sync.WaitGroup (4 воркера, буфер 1000):      %v\n", t3)

	fmt.Println("\n=== ЗАПУСК ЧАНКОВОГО ЧТЕНИЯ (ПО 2500 СТРОК) ===")
	t4 := runChunked(logFileName, chunkSize, false)
	fmt.Printf("4. Чанковое чтение (итоговое время):              %v\n", t4)
}

func generateLogFile(filename string, count int) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	sampleLogs := []string{
		"INFO|2024-01-01 10:00:00|Server started",
		"WARN|2024-01-01 10:01:00|Low memory",
		"ERROR|2024-01-01 10:02:00|Connection failed",
		"INFO|2024-01-01 10:03:00|User login",
		"BROKEN LINE WITHOUT PIPES",
		"ERROR|2024-01-01 10:04:00|Timeout",
	}

	writer := bufio.NewWriter(file)
	for i := 0; i < count; i++ {
		line := sampleLogs[i%len(sampleLogs)]
		if _, err := writer.WriteString(line + "\n"); err != nil {
			return err
		}
	}
	return writer.Flush()
}

func runSingleWorker(filename string, bufSize int) time.Duration {
	start := time.Now()

	file, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	results := make(chan Result, bufSize)

	go func() {
		defer close(results)
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			parts := strings.Split(scanner.Text(), "|")
			if len(parts) == 3 {
				results <- Result{Level: parts[0], Message: parts[2]}
			}
		}
	}()

	var infoCount, warnCount, errorCount int
	for res := range results {
		switch res.Level {
		case "INFO":
			infoCount++
		case "WARN":
			warnCount++
		case "ERROR":
			errorCount++
		}
	}

	return time.Since(start)
}

func runWithWaitGroup(filename string, bufSize int, workersCount int) time.Duration {
	start := time.Now()

	file, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	lines := make(chan string, bufSize)
	results := make(chan Result, bufSize)
	var wg sync.WaitGroup

	go func() {
		defer close(lines)
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			lines <- scanner.Text()
		}
	}()

	for i := 0; i < workersCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for line := range lines {
				parts := strings.Split(line, "|")
				if len(parts) == 3 {
					results <- Result{Level: parts[0], Message: parts[2]}
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	var infoCount, warnCount, errorCount int
	for res := range results {
		switch res.Level {
		case "INFO":
			infoCount++
		case "WARN":
			warnCount++
		case "ERROR":
			errorCount++
		}
	}

	return time.Since(start)
}

func runChunked(filename string, chunkLimit int, silent bool) time.Duration {
	start := time.Now()

	file, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var infoCount, warnCount, errorCount int
	chunkNum := 0

	for {
		var chunk []string
		for len(chunk) < chunkLimit && scanner.Scan() {
			chunk = append(chunk, scanner.Text())
		}

		if len(chunk) == 0 {
			break 
		}

		chunkNum++
		results := make(chan Result, len(chunk))

		go func(lines []string) {
			defer close(results)
			for _, line := range lines {
				parts := strings.Split(line, "|")
				if len(parts) == 3 {
					results <- Result{Level: parts[0], Message: parts[2]}
				}
			}
		}(chunk)

		for res := range results {
			switch res.Level {
			case "INFO":
				infoCount++
			case "WARN":
				warnCount++
			case "ERROR":
				errorCount++
			}
		}

		if !silent {
			total := infoCount + warnCount + errorCount
			errPct := (float64(errorCount) / float64(total)) * 100
			fmt.Printf("Чанк #%d -> INFO: %d | WARN: %d | ERROR: %d | Ошибок: %.1f%%\n",
				chunkNum, infoCount, warnCount, errorCount, errPct)
		}
	}

	return time.Since(start)
}