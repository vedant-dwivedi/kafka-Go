package main
import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"time"
)
func main() {
	listener, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}
	defer func(listener net.Listener) {
		err := listener.Close()
		if err != nil {
			fmt.Println("Failed to close listener")
			os.Exit(1)
		}
	}(listener)
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		// handleConnection(conn)
		go handleConnection(conn)
	}
}
func handleConnection(conn net.Conn) {
	err := conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	defer func(conn net.Conn) {
		fmt.Println("Closing connection: ", conn.RemoteAddr())
		err := conn.Close()
		if err != nil {
			fmt.Println("Error closing connection: ", err.Error())
			os.Exit(1)
		}
	}(conn)
	run := true
	for run {
		req := make([]byte, 512)
		_, err = conn.Read(req)
		if err != nil {
			fmt.Println("Error reading request: ", err.Error())
			// os.Exit(1)
			run = false
		}
		var response = createResponse(req)
		_, err = conn.Write(response)
		if err != nil {
			fmt.Println("Error writing response: ", err.Error())
			run = false
		}
	}
}
/*
message length => 4 bytes
correlation ID => 4 bytes
error code => 2 bytes
entry for the API key 18 (API_VERSIONS) => 1 byte
apikey (18) => 2 bytes
MinVersion for ApiKey 18 (v0) => 2 bytes
MaxVersion for ApiKey 18 (v4) => 2 bytes
Tag Buffer => 1 byte
throttle_time_ms => 4 bytes
Tag Buffer => 1 byte
*/
func createResponse(req []byte) []byte {
	fmt.Println("Creating Response")
	header := req[4:]
	response := make([]byte, 0)
	response = append(response, []byte{0, 0, 0, 19}...) // mssgSize
	response = append(response, header[4:8]...)         // corrId
	var apiVersion int16
	err := binary.Read(bytes.NewReader(header[2:4]), binary.BigEndian, &apiVersion)
	if err != nil {
		return nil
	}
	if apiVersion < 0 || apiVersion > 4 {
		response = append(response, []byte{0, 35}...) // error code '35'
	} else {
		response = append(response, []byte{0, 0}...) // error code '0'
	}
	response = append(response, []byte{2}...)          // one entry
	response = append(response, []byte{0, 18}...)      // apikey
	response = append(response, []byte{0, 0}...)       // min version 0
	response = append(response, []byte{0, 4}...)       // max version 4
	response = append(response, []byte{0}...)          // Tag Buffer
	response = append(response, []byte{0, 0, 0, 0}...) // throttle_time_ms
	response = append(response, []byte{0}...)          // Tag Buffer
	return response
}