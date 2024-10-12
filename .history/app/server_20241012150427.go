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
	// FETCH        uint16 = 1
	// API_VERSIONS uint16 = 18
	FETCH                     uint16 = 1
	API_VERSIONS              uint16 = 18
	DESCRIBE_TOPIC_PARTITIONS uint16 = 75
	// ERRORS
	// NoError               int16 = 0
	// ErrUnSupportedVersion int16 = 35
	// UNKNOWN_TOPIC_ID      int16 = 100
	NoError                 int16 = 0
	ErrUnSupportedVersion   int16 = 35
	UnknownTopicID          int16 = 100
	UnknownTopicOrPartition int16 = 3
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
	// CorrelationID  []byte
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
	// CorrelationID  []byte
	ThrottleTimeMs int32
	ErrorCode      int16
	SessionID      int32
	Responses      []Topic
}
// https://kafka.apache.org/protocol.html#The_Messages_DescribeTopicPartitions <- request format
type DescribeTopicPartitionsResponse struct {
	ThrottleTimeMs         int32
	Topics                 []TopicPartition
	ResponsePartitionLimit int32
	NextCursor             *Cursor
}
type TopicPartition struct {
	ErrorCode                 int16
	Name                      string
	TopicID                   []byte
	IsInternal                bool
	Partitions                []DTPTopicPartition
	TopicAuthorizedOperations int32
}
type DTPTopicPartition struct {
	ErrorCode                   int16
	PartitionIndex              int32
	LeaderID                    int32
	LeaderEpoch                 int32
	ReplicaNodes                []int32
	ISRNodes                    []int32
	EligibleLeaderReplicaNodes  []int32
	LastKnownLeaderReplicaNodes []int32
	OfflineReplicas             []int32
}
type Cursor struct {
	TopicName      string // compact string
	PartitionIndex int32
}
// ================ RESPONSE ===============
// Strategy: Implement interface for each response type
type ResponseType interface {
	// CheckVersion(rqApiVer uint16) int16
	ValidateRequest(req *Request) int16
	FromRequest(req *Request)
	ToBytes() ([]byte, error)
}
// =========== API_VERSIONS ===========
// func (r *APIVersionResponse) CheckVersion(rqApiVer uint16) int16 {
// 	if rqApiVer > 4 {
func (r *APIVersionResponse) ValidateRequest(req *Request) int16 {
	apiVer := binary.BigEndian.Uint16(req.apiVer)
	if apiVer > 4 {
		return ErrUnSupportedVersion
	}
	return NoError
}
func (r *APIVersionResponse) FromRequest(req *Request) {
	// apiVer := binary.BigEndian.Uint16(req.apiVer)
	// r.CorrelationID = req.correlationID
	// r.ErrorCode = r.CheckVersion(apiVer)
	r.ErrorCode = r.ValidateRequest(req)
	r.SupportAPIKeys = ListSupportAPIs[:]
	r.ThrottleTimeMs = 0
}
func (r *APIVersionResponse) ToBytes() ([]byte, error) {
	buf := new(bytes.Buffer)
	// if err := binary.Write(buf, binary.BigEndian, r.CorrelationID); err != nil {
	// 	return nil, err
	// }
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
// func (r *FetchResponse) CheckVersion(rqApiVer uint16) int16 {
// 	if rqApiVer > 16 {
func (r *FetchResponse) ValidateRequest(req *Request) int16 {
	apiVer := binary.BigEndian.Uint16(req.apiVer)
	if apiVer > 16 {
		return ErrUnSupportedVersion
	}
	return NoError
}
func (r *FetchResponse) FromRequest(req *Request) {
	// apiVer := binary.BigEndian.Uint16(req.apiVer)
	// r.CorrelationID = req.correlationID
	// r.ErrorCode = r.CheckVersion(apiVer)
	r.ErrorCode = r.ValidateRequest(req)
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
						// ErrorCode:      UNKNOWN_TOPIC_ID,
						ErrorCode:      UnknownTopicID,
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
	// if err := binary.Write(buf, binary.BigEndian, r.CorrelationID); err != nil {
	// 	return nil, err
	// }
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
// ================ DescribeTopicPartition ===============
func (r *DescribeTopicPartitionsResponse) ValidateRequest(req *Request) int16 {
	apiVer := binary.BigEndian.Uint16(req.apiVer)
	if apiVer != 0 {
		return ErrUnSupportedVersion
	}
	return NoError
}
func (r *DescribeTopicPartitionsResponse) FromRequest(req *Request) {
	// r.ValidateRequest(req)
	r.ThrottleTimeMs = 0
	// parse body
	// first byte is the tagged_fields
	// from 2nd byte is the topic_length - int8
	topicsLength := buffToInt8(req.body[1:2]) - 1
	topics := []TopicPartition{}
	lastPosition := 2
	// parse array of topics
	for i := 0; i < int(topicsLength); i++ {
		// Read CompactString
		nameLength := buffToInt8(req.body[lastPosition:lastPosition+1]) - 1
		lastPosition++
		name := req.body[lastPosition : lastPosition+int(nameLength)]
		lastPosition += int(nameLength)
		topics = append(topics, TopicPartition{
			ErrorCode:                 UnknownTopicOrPartition,
			Name:                      string(name),
			TopicID:                   make([]byte, 16),
			IsInternal:                false,
			Partitions:                []DTPTopicPartition{},
			TopicAuthorizedOperations: 1996,
		})
	}
	r.Topics = topics
	// tagged_fields
	// lastPosition + 1 is the tagged_fields
	lastPosition++
	// lastPosition + 2 is the response_partition_limit
	responsePartitionLimit := binary.BigEndian.Uint32(req.body[lastPosition : lastPosition+4])
	fmt.Println("Response partition limit: ", responsePartitionLimit)
	r.ResponsePartitionLimit = int32(responsePartitionLimit)
	r.NextCursor = nil
}
// https://binspec.org/kafka-describe-topic-partitions-response-v0-unknown-topic?highlight=8-8
func (r *DescribeTopicPartitionsResponse) ToBytes() ([]byte, error) {
	buf := new(bytes.Buffer)
	// tagged_fields
	if err := binary.Write(buf, binary.BigEndian, int8(0)); err != nil {
		return nil, err
	}
	// throttle_time_ms
	if err := binary.Write(buf, binary.BigEndian, r.ThrottleTimeMs); err != nil {
		return nil, err
	}
	// topics length
	if err := binary.Write(buf, binary.BigEndian, int8(len(r.Topics)+1)); err != nil {
		return nil, err
	}
	for _, topic := range r.Topics {
		// error code
		if err := binary.Write(buf, binary.BigEndian, topic.ErrorCode); err != nil {
			return nil, err
		}
		// topic name length
		if err := binary.Write(buf, binary.BigEndian, int8(len(topic.Name)+1)); err != nil {
			return nil, err
		}
		// topic name
		if err := binary.Write(buf, binary.BigEndian, []byte(topic.Name)); err != nil {
			return nil, err
		}
		// topic ID - UUID - 16 bytes
		if err := binary.Write(buf, binary.BigEndian, topic.TopicID); err != nil {
			return nil, err
		}
		// is internal - 1 byte
		if err := binary.Write(buf, binary.BigEndian, topic.IsInternal); err != nil {
			return nil, err
		}
		// partition length - empty
		if err := binary.Write(buf, binary.BigEndian, int8(len(topic.Partitions)+1)); err != nil {
			return nil, err
		}
		// partition array write - if have
		for _, part := range topic.Partitions {
			// error code
			if err := binary.Write(buf, binary.BigEndian, part.ErrorCode); err != nil {
				return nil, err
			}
			// partition index
			if err := binary.Write(buf, binary.BigEndian, part.PartitionIndex); err != nil {
				return nil, err
			}
			// leader id
			if err := binary.Write(buf, binary.BigEndian, part.LeaderID); err != nil {
				return nil, err
			}
			// leader epoch
			if err := binary.Write(buf, binary.BigEndian, part.LeaderEpoch); err != nil {
				return nil, err
			}
			// replica_nodes
			if err := binary.Write(buf, binary.BigEndian, int8(len(part.ReplicaNodes)+1)); err != nil {
				return nil, err
			}
			for _, node := range part.ReplicaNodes {
				if err := binary.Write(buf, binary.BigEndian, node); err != nil {
					return nil, err
				}
			}
			// isr_nodes
			if err := binary.Write(buf, binary.BigEndian, int8(len(part.ISRNodes)+1)); err != nil {
				return nil, err
			}
			for _, node := range part.ISRNodes {
				if err := binary.Write(buf, binary.BigEndian, node); err != nil {
					return nil, err
				}
			}
			// eligible_leader_replica_nodes
			if err := binary.Write(buf, binary.BigEndian, int8(len(part.EligibleLeaderReplicaNodes)+1)); err != nil {
				return nil, err
			}
			for _, node := range part.EligibleLeaderReplicaNodes {
				if err := binary.Write(buf, binary.BigEndian, node); err != nil {
					return nil, err
				}
			}
			// last_known_leader_replica_nodes
			if err := binary.Write(buf, binary.BigEndian, int8(len(part.LastKnownLeaderReplicaNodes)+1)); err != nil {
				return nil, err
			}
			for _, node := range part.LastKnownLeaderReplicaNodes {
				if err := binary.Write(buf, binary.BigEndian, node); err != nil {
					return nil, err
				}
			}
			// offline_replicas
			if err := binary.Write(buf, binary.BigEndian, int8(len(part.OfflineReplicas)+1)); err != nil {
				return nil, err
			}
			for _, node := range part.OfflineReplicas {
				if err := binary.Write(buf, binary.BigEndian, node); err != nil {
					return nil, err
				}
			}
			// tagged_fields
			if err := binary.Write(buf, binary.BigEndian, int8(0)); err != nil {
				return nil, err
			}
		}
		// topic_authorized_operations - 4 bytes
		if err := binary.Write(buf, binary.BigEndian, topic.TopicAuthorizedOperations); err != nil {
			return nil, err
		}
		// tag_buffer
		if err := binary.Write(buf, binary.BigEndian, int8(0)); err != nil {
			return nil, err
		}
	}
	// Write next cursor
	if r.NextCursor != nil {
		topicName := []byte(r.NextCursor.TopicName)
		// topic name length
		if err := binary.Write(buf, binary.BigEndian, int8(len(topicName)+1)); err != nil {
			return nil, err
		}
		// topic name
		if err := binary.Write(buf, binary.BigEndian, topicName); err != nil {
			return nil, err
		}
		// partition index
		if err := binary.Write(buf, binary.BigEndian, r.NextCursor.PartitionIndex); err != nil {
			return nil, err
		}
	} else {
		// write null cursor
		// next cursor = 0xff -> null
		if err := binary.Write(buf, binary.BigEndian, []byte{0xff}); err != nil {
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
	case DESCRIBE_TOPIC_PARTITIONS:
		describeTopicPartitionsRequest := &DescribeTopicPartitionsResponse{}
		response = initResponse(describeTopicPartitionsRequest)
	default:
		// return nil, fmt.Errorf("unsupported request type: %d", requestApiKey)
		return []byte{}, nil
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
		fmt.Println("Error handling request: ", err)
		return
	}
	// binary.Write(c, binary.BigEndian, int32(len(resp)))
	// auto add length of message
	// len(resp) = body length + 4 bytes for correlation_id
	binary.Write(c, binary.BigEndian, int32(len(resp)+4))
	// write correlation_id
	binary.Write(c, binary.BigEndian, request.correlationID)
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
// ===================== HELPER =====================
func buffToInt8(buff []byte) int8 {
	var n int8
	if err := binary.Read(bytes.NewReader(buff), binary.BigEndian, &n); err != nil {
		fmt.Println("Error converting bytes to int8: ", err)
		return 0
	}
	return n
}