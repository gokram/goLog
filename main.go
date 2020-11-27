package main

import (
    "fmt"
    "os"
    "time"

	"bytes"
	"log"
	"net/http"
	"io/ioutil"
//	"code.cloudfoundry.org/rfc5424"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awsutil"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
//	"github.com/aws/aws-sdk-go/service/s3/s3manager"

"github.com/tidwall/gjson"

)


// updated with object store service key
const (
//        WORKING_PATH = "C:/tmp/path_working/"
WORKING_PATH = "./path_working/"
BUFFER_RAW = 100
TIMEOUT_SEC = 900
FILENAME_PREFIX = "scp_log_and_metrics_"

S3_PREFIX = "/goLog/"
)



func main() {


vcap_services := os.Getenv("VCAP_SERVICES")
	if vcap_services == "" {
		fmt.Printf("No VCAP_SERVICES")
		os.Exit(1)
	}
	

//TODO ERROR MNGMTN

OBJECTSTORE :=  gjson.Get(vcap_services, "objectstore.0.name").String()
INSTANCE_NAME :=  gjson.Get(vcap_services, "objectstore.0.instance_name").String()
s3_region :=  gjson.Get(vcap_services, "objectstore.0.credentials.region").String()
s3_bucket :=  gjson.Get(vcap_services, "objectstore.0.credentials.bucket").String()
aws_access_key_id :=  gjson.Get(vcap_services, "objectstore.0.credentials.access_key_id").String()
aws_secret_access_key := gjson.Get(vcap_services, "objectstore.0.credentials.secret_access_key").String()

fmt.Println("OBJECTSTORE :", OBJECTSTORE)
fmt.Println("INSTANCE_NAME :", INSTANCE_NAME)

channel_filename := make(chan string,BUFFER_RAW)

fmt.Println("main -> START")

handler:= NewSyslog()

//go dummy_HTTPListener(channel_raw)
//fmt.Println("main -> HTTP step")

go buildBatch(handler.messages, channel_filename)
fmt.Println("main -> buildBatch step")

go uploadS3(channel_filename, s3_bucket, aws_access_key_id, aws_secret_access_key, s3_region)

http.ListenAndServe(fmt.Sprintf(":%s", os.Getenv("PORT")), handler)
//http.ListenAndServe(":9000", handler)
fmt.Println("main -> HTTP step")

fmt.Println("main -> END")

//time.Sleep(60* time.Second)
}


type Handler struct {
	messages chan string
	
}

func NewSyslog() *Handler {
	return &Handler{
		messages: make(chan string,BUFFER_RAW),
	}
}

// FUNCTION -----------------------------------

func dummy_HTTPListener(channel_raw chan string) {
	
 fmt.Println("dummy_HTTPListener -> START")
for
	{
	
	channel_raw <- "pippo"

 fmt.Println("dummy_HTTPListener -> emit")

	} //end for
} //end funct


func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		log.Printf("Failed to read body: %s", err)
		return
	}

	if len(body) < 1 {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("Empty body")
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

func buildBatch(channel_raw chan string, channel_filename chan string) {


fmt.Println("buildBatch -> START")

// FOR START
for {

	// CREATE FILE ----------------------

	fmt.Println("buildBatch -> FOR ")

	now := time.Now()
	currentTime := now.Format("20060102150405")

	fmt.Println("Current Time in String: %s", currentTime)

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
	        fmt.Println("file closed: %s",filefullname)
        	}

	} else {
        fmt.Println("File already exists!", filefullname)
        return
	    }

    fmt.Println("File created successfully", filefullname)
	
// LOOP FOR SOME SECONDS

loop:
    for timeout := time.After(time.Second * TIMEOUT_SEC); ; {
        select {
        case <-timeout:
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
fmt.Println("File updated")

	cerr:= file.Close()
	    if cerr != nil {
        	fmt.Println(cerr)
	        return
    		} else {
	        fmt.Println("file closed: %s",filefullname)
        	
	    	}


        } //end select  

    } //end for loop



channel_filename <- filename
} // END FOR

} //end funct


// FUNCTION -----------------------------------

func uploadS3(channel_filename chan string, s3_bucket string, aws_access_key_id string, aws_secret_access_key string, s3_region string) {
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

	uResp, err := uploadFileToS3(client, key, filefullname,s3_bucket)
	if err != nil {
		log.Fatalf("failed to upload file - %v", err)
fmt.Println("failed to upload file - %v", err)

	}
	log.Printf("uploaded file with response %s", uResp)
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

func uploadFileToS3(s3Client *s3.S3, key string, filePath string,s3_bucket string,) (string, error) {
	
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