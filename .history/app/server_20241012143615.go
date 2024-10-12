package main
import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"os"
)
// https://kafka.apache.org/protocol.html#protocol_error_codes
const (
	byteSize = 1024
	// CONSTANTS
	// REQUEST_TYPE
	FETCH        uint16 = 1
	API_VERSIONS uint16 = 18
	// ERRORS
	NoError               int16 = 0
	ErrUnSupportedVersion int16 = 35
	UNKNOWN_TOPIC_ID      int16 = 100
)
// Doc https://kafka.apache.org/protocol.html#protocol_messages
// Check headers v2
// First 4 bytes are length of the message
// Doc about type and their values https://kafka.apache.org/protocol.html#protocol_types
type Request struct {
	length        []byte // buff[0:4]
	apiKey        []byte // buff[4:6]
	apiVer        []byte // buff[6:8]
	correlationID []byte // buff[8:12]
	// clientIDLength []byte // buff[12:14]
	// clientIDData []byte // buff[14:14+clientIDLength]
	clientID *string
	body     []byte // buff[14+clientIDLength:]
}
// API Versions
type SupportAPI struct {
	APIKey     int16
	MinVersion int16
	MaxVersion int16
}
var ListSupportAPIs = [...]SupportAPI{
	{
		APIKey:     1,
		MinVersion: 15,
		MaxVersion: 16,
	},
	{
		APIKey:     18,
		MinVersion: 3,
		MaxVersion: 4,
	},
	{
		APIKey:     75,
		MinVersion: 0,
		MaxVersion: 0,
	},
}
// https://kafka.apache.org/protocol.html#The_Messages_ApiVersions <- response format
type APIVersionResponse struct {
	CorrelationID  []byte
	ErrorCode      int16
	SupportAPIKeys []SupportAPI
	ThrottleTimeMs int32
}
// Fetch Request
type Topic struct {
	TopicID    []byte
	Partitions []Partition
}
type Partition struct {
	PartitionIndex int32
	ErrorCode      int16
}
// Fetch Response (Version: 16) => throttle_time_ms error_code session_id [responses] TAG_BUFFER
type FetchResponse struct {
	CorrelationID  []byte
	ThrottleTimeMs int32
	ErrorCode      int16
	SessionID      int32
	Responses      []Topic
}
// ================ RESPONSE ===============
// Strategy: Implement interface for each response type
type ResponseType interface {
	CheckVersion(rqApiVer uint16) int16
	FromRequest(req *Request)
	ToBytes() ([]byte, error)
}
// =========== API_VERSIONS ===========
func (r *APIVersionResponse) CheckVersion(rqApiVer uint16) int16 {
	if rqApiVer > 4 {
		return ErrUnSupportedVersion
	}
	return NoError
}
func (r *APIVersionResponse) FromRequest(req *Request) {
	apiVer := binary.BigEndian.Uint16(req.apiVer)
	r.CorrelationID = req.correlationID
	r.ErrorCode = r.CheckVersion(apiVer)
	r.SupportAPIKeys = ListSupportAPIs[:]
	r.ThrottleTimeMs = 0
}
func (r *APIVersionResponse) ToBytes() ([]byte, error) {
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.BigEndian, r.CorrelationID); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, r.ErrorCode); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, int8(len(r.SupportAPIKeys)+1)); err != nil {
		return nil, err
	}
	for _, key := range r.SupportAPIKeys {
		if err := binary.Write(buf, binary.BigEndian, key.APIKey); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.BigEndian, key.MinVersion); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.BigEndian, key.MaxVersion); err != nil {
			return nil, err
		}
		// tagged_fields
		if err := binary.Write(buf, binary.BigEndian, int8(0)); err != nil {
			return nil, err
		}
	}
	if err := binary.Write(buf, binary.BigEndian, r.ThrottleTimeMs); err != nil {
		return nil, err
	}
	// tagged_fields
	if err := binary.Write(buf, binary.BigEndian, int8(0)); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
// =========== FETCH ===========
func (r *FetchResponse) CheckVersion(rqApiVer uint16) int16 {
	if rqApiVer > 16 {
		return ErrUnSupportedVersion
	}
	return NoError
}
func (r *FetchResponse) FromRequest(req *Request) {
	apiVer := binary.BigEndian.Uint16(req.apiVer)
	r.CorrelationID = req.correlationID
	r.ErrorCode = r.CheckVersion(apiVer)
	r.ThrottleTimeMs = 0
	r.SessionID = 0
	// parse body
	maxWaitMs := req.body[0:4]
	minBytes := req.body[4:8]
	maxBytes := req.body[8:12]
	isolationLevel := req.body[12:13]
	sessionID := req.body[13:17]
	sessionEpoch := req.body[17:21]
	topicLength := req.body[21:23]
	topicID := req.body[23 : 23+16]
	fmt.Printf("Parse body: %x-%x-%x-%x-%x-%x-%x-%x\n", maxWaitMs, minBytes, maxBytes, isolationLevel, sessionID, sessionEpoch, topicLength, topicID)
	var topics []Topic
	if binary.BigEndian.Uint16(topicLength)-1 > 0 {
		topics = []Topic{
			{
				TopicID: topicID,
				Partitions: []Partition{
					{
						PartitionIndex: 0,
						ErrorCode:      UNKNOWN_TOPIC_ID,
					},
				},
			},
		}
	}
	r.Responses = topics
}
func (r *FetchResponse) ToBytes() ([]byte, error) {
	buf := new(bytes.Buffer)
	// write correlation_id
	if err := binary.Write(buf, binary.BigEndian, r.CorrelationID); err != nil {
		return nil, err
	}
	// tagged_fields
	if err := binary.Write(buf, binary.BigEndian, int8(0)); err != nil {
		return nil, err
	}
	// write throttle_time_ms
	if err := binary.Write(buf, binary.BigEndian, r.ThrottleTimeMs); err != nil {
		return nil, err
	}
	// write error_code
	if err := binary.Write(buf, binary.BigEndian, r.ErrorCode); err != nil {
		return nil, err
	}
	// write session_id
	if err := binary.Write(buf, binary.BigEndian, r.SessionID); err != nil {
		return nil, err
	}
	// write num_of_responses
	if err := binary.Write(buf, binary.BigEndian, int8(len(r.Responses)+1)); err != nil {
		return nil, err
	}
	for _, topic := range r.Responses {
		// write topic_id
		if err := binary.Write(buf, binary.BigEndian, topic.TopicID); err != nil {
			return nil, err
		}
		// write num_of_partitions
		if err := binary.Write(buf, binary.BigEndian, int8(len(topic.Partitions)+1)); err != nil {
			return nil, err
		}
		for _, part := range topic.Partitions {
			// write partition_index
			if err := binary.Write(buf, binary.BigEndian, part.PartitionIndex); err != nil {
				return nil, err
			}
			// write error_code
			if err := binary.Write(buf, binary.BigEndian, part.ErrorCode); err != nil {
				return nil, err
			}
			// write high_watermark
			if err := binary.Write(buf, binary.BigEndian, int64(0)); err != nil {
				return nil, err
			}
			// write last_stable_offset
			if err := binary.Write(buf, binary.BigEndian, int64(0)); err != nil {
				return nil, err
			}
			// write log_start_offset
			if err := binary.Write(buf, binary.BigEndian, int64(0)); err != nil {
				return nil, err
			}
			// write aborted_transactions - length 0
			if err := binary.Write(buf, binary.BigEndian, int8(1)); err != nil {
				return nil, err
			}
			// // write producer_id
			// if err := binary.Write(buf, binary.BigEndian, int64(0)); err != nil {
			// 	return nil, err
			// }
			// // write first_offset
			// if err := binary.Write(buf, binary.BigEndian, int64(0)); err != nil {
			// 	return nil, err
			// }
			// _tagged_fields
			if err := binary.Write(buf, binary.BigEndian, int8(0)); err != nil {
				return nil, err
			}
			// write preferred_read_replica
			if err := binary.Write(buf, binary.BigEndian, int32(0)); err != nil {
				return nil, err
			}
			// write records_size
			if err := binary.Write(buf, binary.BigEndian, int8(0)); err != nil {
				return nil, err
			}
			// _tagged_fields
			// if err := binary.Write(buf, binary.BigEndian, int8(0)); err != nil {
			// 	return nil, err
			// }
		}
		// tag_buffer 1 byte - end of topic
		if err := binary.Write(buf, binary.BigEndian, int8(0)); err != nil {
			return nil, err
		}
	}
	// tag_buffer
	if err := binary.Write(buf, binary.BigEndian, int8(0)); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
type Response struct {
	responseType ResponseType
}
func initResponse(rsType ResponseType) *Response {
	return &Response{
		responseType: rsType,
	}
}
func (r *Response) FromRequest(req *Request) {
	r.responseType.FromRequest(req)
}
func (r *Response) ToBytes() ([]byte, error) {
	return r.responseType.ToBytes()
}
// ================ END RESPONSE ===============
func ReadRequest(c net.Conn) (*Request, error) {
	buff := make([]byte, byteSize)
	_, err := c.Read(buff)
	if err != nil {
		if err.Error() != "EOF" {
			return nil, fmt.Errorf("error reading from connection: %w", err)
		}
	}
	// parse clientID
	clientIDLength := int16(buff[12])<<8 + int16(buff[13])
	var clientID *string
	if clientIDLength != -1 {
		clientIDData := buff[14 : 14+clientIDLength]
		clientIDStr := string(clientIDData)
		clientID = &clientIDStr
	}
	req := &Request{
		length:        buff[0:4],
		apiKey:        buff[4:6],
		apiVer:        buff[6:8],
		correlationID: buff[8:12],
		clientID:      clientID,
		body:          buff[14+clientIDLength:],
	}
	return req, nil
}
func HandleRequest(request *Request) ([]byte, error) {
	requestApiKey := binary.BigEndian.Uint16(request.apiKey)
	var response *Response
	switch requestApiKey {
	case API_VERSIONS:
		apiVersionRequest := &APIVersionResponse{}
		response = initResponse(apiVersionRequest)
	case FETCH:
		fetchRequest := &FetchResponse{}
		response = initResponse(fetchRequest)
	default:
		return nil, fmt.Errorf("unsupported request type: %d", requestApiKey)
	}
	response.FromRequest(request)
	resp, err := response.ToBytes()
	if err != nil {
		return nil, fmt.Errorf("error converting response to bytes: %w", err)
	}
	return resp, nil
}
func WriteResponse(c net.Conn, request *Request) {
	resp, err := HandleRequest(request)
	if err != nil {
		// fmt.Println("Error handling request: ", err)
		return
	}
	binary.Write(c, binary.BigEndian, int32(len(resp)))
	binary.Write(c, binary.BigEndian, resp)
}
func handleConnection(conn net.Conn) {
	defer conn.Close()
	// handle multiple request from connection
	for {
		// read request
		req, err := ReadRequest(conn)
		if err != nil {
			fmt.Println("Error reading from connection: ", err.Error())
			continue
		}
		// write response
		WriteResponse(conn, req)
	}
}
func main() {
	fmt.Println("Logs from your program will appear here!")
	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}
	defer l.Close()
	// handle multiple client connections
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue // Skip to the next connection
		}
		// Handle connections concurrently
		go handleConnection(conn)
	}
}