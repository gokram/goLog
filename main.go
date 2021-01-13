package main

import (
    	"fmt"
    	"os"
	"time"
	"bytes"
	"log"
	"net/http"
	"io/ioutil"
	"encoding/json"
	"strconv"
	"os/signal"
	"syscall"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awsutil"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"

// NOT USED:
//	"code.cloudfoundry.org/rfc5424"
//	"github.com/aws/aws-sdk-go/service/s3/s3manager"

)



func main() {


vcap_services := os.Getenv("VCAP_SERVICES")
	if vcap_services == "" {
		fmt.Printf("No VCAP_SERVICES")
		os.Exit(1)
	}

vcap_application := os.Getenv("VCAP_APPLICATION")
	if vcap_services == "" {
		fmt.Printf("No VCAP_APPLICATION")
		os.Exit(1)
	}

fmt.Println("TEST NEW JSON :  -> vcap_services:", vcap_services)

//TEST NEW JSON


var objectstore_data map[string]interface{}


err := json.Unmarshal([]byte(vcap_services), &objectstore_data)

	if err != nil {
		log.Printf("Failed to unmarshal (via JSON) message (%s): %s", string(vcap_services), err)
		return
	}

objstore := objectstore_data["objectstore"].([]interface{})[0].(map[string]interface{})
OBJECTSTORE := objstore["name"].(string)
INSTANCE_NAME := objstore["instance_name"].(string)

    cred := objectstore_data["objectstore"].([]interface{})[0].(map[string]interface{})["credentials"].(map[string]interface{})
    s3_region:= cred["region"].(string)
s3_bucket:= cred["bucket"].(string)
aws_access_key_id := cred["access_key_id"].(string)
aws_secret_access_key := cred["secret_access_key"].(string)


fmt.Println("OBJECTSTORE :", OBJECTSTORE)
fmt.Println("INSTANCE_NAME :", INSTANCE_NAME)

//READ ENV VARIABLE from manifest.yml
var WORKING_PATH = os.Getenv("TMPDIR") + "/"
fmt.Println("Variable WORKING_PATH :", WORKING_PATH)

BUFFER_RAW, err := strconv.ParseInt(os.Getenv("BUFFER_RAW"), 0, 64)
if err != nil {
    panic(err)
}
fmt.Println(BUFFER_RAW)

TIMEOUT_SEC, err := strconv.ParseInt(os.Getenv("TIMEOUT_SEC"), 0, 64)
if err != nil {
    panic(err)
}
fmt.Println(TIMEOUT_SEC)
var INDEX = os.Getenv("CF_INSTANCE_INDEX")


var application_data map[string]interface{}


err_2 := json.Unmarshal([]byte(vcap_application), &application_data)

	if err_2 != nil {
		log.Printf("Failed to unmarshal (via JSON) message (%s): %s", string(vcap_application), err_2)
		return
	}

application_name := application_data["application_name"].(string)
organization_name := application_data["organization_name"].(string)
space_name := application_data["space_name"].(string)
fmt.Println("TEST NEW JSON :  -> application_name:", application_name)
fmt.Println("TEST NEW JSON :  -> organization_name:", organization_name)
fmt.Println("TEST NEW JSON :  -> space_name:", space_name)

//var FILENAME_PREFIX = os.Getenv("FILENAME_PREFIX") + "INDX_" + INDEX + "_"
var FILENAME_PREFIX = application_name + "_" + os.Getenv("FILENAME_PREFIX") + "_INDX_" + INDEX + "_"

//var S3_PREFIX = os.Getenv("S3_PREFIX")
var S3_PREFIX = organization_name + "/" + space_name + "/"
fmt.Println("Variable S3_PREFIX :", S3_PREFIX)

channel_filename := make(chan string,BUFFER_RAW)

fmt.Println("main -> START")

handler:= NewSyslog(BUFFER_RAW)

//go dummy_HTTPListener(channel_raw)
//fmt.Println("main -> HTTP step")


//SIGNAL
sigs := make(chan os.Signal, 1)
signal.Notify(sigs, syscall.SIGTERM)
gracefulshutdown := make(chan bool,1)
go func() {
	<-sigs
	//cancel()
	gracefulshutdown <- true
	fmt.Println("MAIN - XXX Signal Received -- graceful shutdown -- S3 upload immediatly --- MAIN")	
	handler.messages <- "goLog - XXX Signal Received -- graceful shutdown -- S3 upload immediatly --- MAIN" 
	for { 
	time.Sleep(1 * time.Second) 
	fmt.Println("MAIN - XXX Signal Received -- Waiting on instance %d", INDEX)	
	}

}()

go buildBatch(handler.messages, channel_filename, FILENAME_PREFIX, WORKING_PATH, TIMEOUT_SEC, gracefulshutdown)
fmt.Println("MAIN - main -> buildBatch step")

go uploadS3(channel_filename, s3_bucket, aws_access_key_id, aws_secret_access_key, s3_region, WORKING_PATH, S3_PREFIX)


http.ListenAndServe(fmt.Sprintf(":%s", os.Getenv("PORT")), handler)
//http.ListenAndServe(":9000", handler)
fmt.Println("MAIN - main -> HTTP step")

fmt.Println("MAIN - main -> END")

//time.Sleep(60* time.Second)
}


type Handler struct {
	messages chan string
	
}

func NewSyslog(BUFFER_RAW int64) *Handler {
	return &Handler{
		messages: make(chan string,BUFFER_RAW),
	}
}

// FUNCTION -----------------------------------


func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		log.Printf("ServeHTTP - Failed to read body: %s", err)
		return
	}

	if len(body) < 1 {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("ServeHTTP - Empty body")
		return
	}

  // PARSE RFC 5424 Message in order to extract only MSG part from body
//   body = HEADER STRUCTURED-DATA MSG

// NOTE: This is not a log line. We want the messages printed to STDOUT.

//	fmt.Printf("Received: %s", string(body))

//	msg := rfc5424.Message{}
//	err = msg.UnmarshalBinary(body)
//	if err != nil {
//		log.Printf("Failed to unmarshal (via RFC-5424) message: %s", err)
//		w.WriteHeader(http.StatusBadRequest)
//		return
//	}
  //  h.messages <- string(msg.Message)	

//fmt.Printf("ProcessID: %s", string(msg.ProcessID))


//EXTRACT ALL body, without parsing rfc 5424 message

 h.messages <- string(body)	

	return
}




// FUNCTION -----------------------------------

func buildBatch(channel_raw chan string, channel_filename chan string, FILENAME_PREFIX string, WORKING_PATH string, TIMEOUT_SEC int64, gracefulshutdown chan bool) {


fmt.Println("buildBatch -> START")


// FOR START
for {

	// CREATE FILE ----------------------

	fmt.Println("BUILDBATCH - buildBatch -> FOR ")

	now := time.Now()
	currentTime := now.Format("20060102150405")

	fmt.Println("BUILDBATCH - Current Time in String: %s", currentTime)

	filename := FILENAME_PREFIX + currentTime + ".txt"

	filefullname := WORKING_PATH + filename

	var _, err = os.Stat(filefullname)

	if os.IsNotExist(err)	{
        file, err := os.Create(filefullname )
        	if err != nil {
            	fmt.Println(err)
            	return
	        }
      //  defer file.Close()

	cerr:= file.Close()
  	  if cerr != nil {
        	fmt.Println(cerr)
        	return
    		} else {
	        fmt.Println("BUILDBATCH - file closed: %s",filefullname)
        	}

	} else {
        fmt.Println("BUILDBATCH - File already exists!", filefullname)
        return
	    }

    fmt.Println("BUILDBATCH - File created successfully", filefullname)
	
// LOOP FOR SOME SECONDS

loop:
for timeout := time.After(time.Second * time.Duration(TIMEOUT_SEC)); ; {

     select {
        case <-timeout:
		fmt.Println("BUILDBATCH - Timeout triggered in loop-select UpdateBatch")
		break loop
        case <-gracefulshutdown:
		fmt.Println("BUILDBATCH - XXX Signal Received -- graceful shutdown -- S3 upload immediatly ---  UpdateBatch")
		break loop
        default:

// APPEND DATA to EXIXSTING FILE ---------
   
    file, err := os.OpenFile(filefullname, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0660)
    if err != nil {
        fmt.Println(err)
        return
    	}
   // defer file.Close()

	message := <-channel_raw
   	fmt.Fprintf(file, "%s\n", message )
	fmt.Println("BUILDBATCH - File updated")

	cerr:= file.Close()
	    if cerr != nil {
        	fmt.Println(cerr)
	        return
    		} else {
	        fmt.Println("BUILDBATCH - file closed: %s",filefullname)
        	
	    	}


        } //end select  

fmt.Println("BUILDBATCH - Exited SELECT")

    } //end fortimeout/loop

fmt.Println("BUILDBATCH - Exited FOR LOOP")

channel_filename <- filename

} // END FOR

} //end funct


// FUNCTION -----------------------------------

func uploadS3(channel_filename chan string, s3_bucket string, aws_access_key_id string, aws_secret_access_key string, s3_region string, WORKING_PATH string, S3_PREFIX string) {
for
  {

filename :=<- channel_filename

filefullname := WORKING_PATH + filename

fmt.Println("uploadS3 - START")
// WRITE TO S3

	client := getS3Client(aws_access_key_id, aws_secret_access_key, s3_region)

	key := filename
	//fileName := filename

 fmt.Println("Uploading file - %s", filefullname)

	uResp, err := uploadFileToS3(client, key, filefullname,s3_bucket, S3_PREFIX)
	if err != nil {
		log.Fatalf("failed to upload file - %v", err)
fmt.Println("failed to upload file - %v", err)

	}
	//log.Printf("uploaded file with response %s", uResp)
fmt.Println("uploaded file with response %s", uResp)

    // Removing file from the directory 
    // Using Remove() function 
     e := os.Remove(filefullname) 
    if e != nil { 
        log.Fatal(e) 
     fmt.Println("failed to Remove file - %v", e)
     
     }

fmt.Println("uploadS3 - END")
 }// end for

} // end func


// S3 FUNCTIONS --------------------

func uploadFileToS3(s3Client *s3.S3, key string, filePath string,s3_bucket string, S3_PREFIX string) (string, error) {
	
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("failed to open the file - %v", err)
		return "", err
	}
	defer file.Close()

	fileInfo, _ := file.Stat()
	size := fileInfo.Size()
	buffer := make([]byte, size)

	file.Read(buffer)
	reader := bytes.NewReader(buffer)
	fileType := http.DetectContentType(buffer)
	path := S3_PREFIX + key // or it can also be file.Name()
	params := &s3.PutObjectInput{
		Bucket:        aws.String(s3_bucket),
		Key:           aws.String(path),
		Body:          reader,
		ContentLength: aws.Int64(size),
		ContentType:   aws.String(fileType),
	}

	resp, err := s3Client.PutObject(params)
	if err != nil {
		log.Fatalf("failed to put the file into the bucket - %v", err)
		return "", err
	}

	return awsutil.StringValue(resp), nil
}

func getS3Client(aws_access_key_id string, aws_secret_access_key string, s3_region string) *s3.S3 {
	token := ""
	creds := credentials.NewStaticCredentials(aws_access_key_id, aws_secret_access_key, token)
	_, err := creds.Get()
	if err != nil {
		log.Fatalf("failed to use credentials %v", err)
	}

	cfg := aws.NewConfig().WithRegion(s3_region).WithCredentials(creds)
	svc := s3.New(session.New(), cfg)
	return svc
}