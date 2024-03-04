package frontend

import (
	"bufio"
	"fmt"
	"log"
	"os"
)

func run() {
	fmt.Println("Starting...")
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "q" {
			fmt.Printf("Exiting the program...")
			break
		}
		log.Println(line)
	}
}
