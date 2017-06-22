package main

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/s3"
)

type Message struct {
	Header Header `json:"messageHeader"`
	Body   Body   `json:"messageBody"`
}

type Header struct {
	ID    string `json:"messageId"`
	Type  string `json:"messageType"`
	Class string `json:"messageClass"`
}

type Body struct {
	UUID  string  `json:"objectUuid"`
	Title string  `json:"objectTitle"`
	Files []*File `json:"objectFile"`
}

type File struct {
	ID          string `json:"fileUuid"`
	Path        string `json:"fileStorageLocation"`
	StorageType string `json:"fileStorageType"`
	Title       string `json:"fileName"`
}

const html = `<!DOCTYPE html>
<html>
	<head>
		<meta charset="utf-8">
		<title>RDSS Archivematica Msgcreator</title>
		<style type="text/css">
			*,*:after,*:before{box-sizing:inherit}html{box-sizing:border-box;font-size:62.5%}body{color:#606c76;font-family:'Roboto', 'Helvetica Neue', 'Helvetica', 'Arial', sans-serif;font-size:1.6em;font-weight:300;letter-spacing:.01em;line-height:1.6}blockquote{border-left:0.3rem solid #d1d1d1;margin-left:0;margin-right:0;padding:1rem 1.5rem}blockquote *:last-child{margin-bottom:0}.button,button,input[type='button'],input[type='reset'],input[type='submit']{background-color:#9b4dca;border:0.1rem solid #9b4dca;border-radius:.4rem;color:#fff;cursor:pointer;display:inline-block;font-size:1.1rem;font-weight:700;height:3.8rem;letter-spacing:.1rem;line-height:3.8rem;padding:0 3.0rem;text-align:center;text-decoration:none;text-transform:uppercase;white-space:nowrap}.button:focus,.button:hover,button:focus,button:hover,input[type='button']:focus,input[type='button']:hover,input[type='reset']:focus,input[type='reset']:hover,input[type='submit']:focus,input[type='submit']:hover{background-color:#606c76;border-color:#606c76;color:#fff;outline:0}.button[disabled],button[disabled],input[type='button'][disabled],input[type='reset'][disabled],input[type='submit'][disabled]{cursor:default;opacity:.5}.button[disabled]:focus,.button[disabled]:hover,button[disabled]:focus,button[disabled]:hover,input[type='button'][disabled]:focus,input[type='button'][disabled]:hover,input[type='reset'][disabled]:focus,input[type='reset'][disabled]:hover,input[type='submit'][disabled]:focus,input[type='submit'][disabled]:hover{background-color:#9b4dca;border-color:#9b4dca}.button.button-outline,button.button-outline,input[type='button'].button-outline,input[type='reset'].button-outline,input[type='submit'].button-outline{background-color:transparent;color:#9b4dca}.button.button-outline:focus,.button.button-outline:hover,button.button-outline:focus,button.button-outline:hover,input[type='button'].button-outline:focus,input[type='button'].button-outline:hover,input[type='reset'].button-outline:focus,input[type='reset'].button-outline:hover,input[type='submit'].button-outline:focus,input[type='submit'].button-outline:hover{background-color:transparent;border-color:#606c76;color:#606c76}.button.button-outline[disabled]:focus,.button.button-outline[disabled]:hover,button.button-outline[disabled]:focus,button.button-outline[disabled]:hover,input[type='button'].button-outline[disabled]:focus,input[type='button'].button-outline[disabled]:hover,input[type='reset'].button-outline[disabled]:focus,input[type='reset'].button-outline[disabled]:hover,input[type='submit'].button-outline[disabled]:focus,input[type='submit'].button-outline[disabled]:hover{border-color:inherit;color:#9b4dca}.button.button-clear,button.button-clear,input[type='button'].button-clear,input[type='reset'].button-clear,input[type='submit'].button-clear{background-color:transparent;border-color:transparent;color:#9b4dca}.button.button-clear:focus,.button.button-clear:hover,button.button-clear:focus,button.button-clear:hover,input[type='button'].button-clear:focus,input[type='button'].button-clear:hover,input[type='reset'].button-clear:focus,input[type='reset'].button-clear:hover,input[type='submit'].button-clear:focus,input[type='submit'].button-clear:hover{background-color:transparent;border-color:transparent;color:#606c76}.button.button-clear[disabled]:focus,.button.button-clear[disabled]:hover,button.button-clear[disabled]:focus,button.button-clear[disabled]:hover,input[type='button'].button-clear[disabled]:focus,input[type='button'].button-clear[disabled]:hover,input[type='reset'].button-clear[disabled]:focus,input[type='reset'].button-clear[disabled]:hover,input[type='submit'].button-clear[disabled]:focus,input[type='submit'].button-clear[disabled]:hover{color:#9b4dca}code{background:#f4f5f6;border-radius:.4rem;font-size:86%;margin:0 .2rem;padding:.2rem .5rem;white-space:nowrap}pre{background:#f4f5f6;border-left:0.3rem solid #9b4dca;overflow-y:hidden}pre>code{border-radius:0;display:block;padding:1rem 1.5rem;white-space:pre}hr{border:0;border-top:0.1rem solid #f4f5f6;margin:3.0rem 0}input[type='email'],input[type='number'],input[type='password'],input[type='search'],input[type='tel'],input[type='text'],input[type='url'],textarea,select{-webkit-appearance:none;-moz-appearance:none;appearance:none;background-color:transparent;border:0.1rem solid #d1d1d1;border-radius:.4rem;box-shadow:none;box-sizing:inherit;height:3.8rem;padding:.6rem 1.0rem;width:100%}input[type='email']:focus,input[type='number']:focus,input[type='password']:focus,input[type='search']:focus,input[type='tel']:focus,input[type='text']:focus,input[type='url']:focus,textarea:focus,select:focus{border-color:#9b4dca;outline:0}select{background:url('data:image/svg+xml;utf8,<svg xmlns="http://www.w3.org/2000/svg" height="14" viewBox="0 0 29 14" width="29"><path fill="#d1d1d1" d="M9.37727 3.625l5.08154 6.93523L19.54036 3.625"/></svg>') center right no-repeat;padding-right:3.0rem}select:focus{background-image:url('data:image/svg+xml;utf8,<svg xmlns="http://www.w3.org/2000/svg" height="14" viewBox="0 0 29 14" width="29"><path fill="#9b4dca" d="M9.37727 3.625l5.08154 6.93523L19.54036 3.625"/></svg>')}textarea{min-height:6.5rem}label,legend{display:block;font-size:1.6rem;font-weight:700;margin-bottom:.5rem}fieldset{border-width:0;padding:0}input[type='checkbox'],input[type='radio']{display:inline}.label-inline{display:inline-block;font-weight:normal;margin-left:.5rem}.container{margin:0 auto;max-width:112.0rem;padding:0 2.0rem;position:relative;width:100%}.row{display:flex;flex-direction:column;padding:0;width:100%}.row.row-no-padding{padding:0}.row.row-no-padding>.column{padding:0}.row.row-wrap{flex-wrap:wrap}.row.row-top{align-items:flex-start}.row.row-bottom{align-items:flex-end}.row.row-center{align-items:center}.row.row-stretch{align-items:stretch}.row.row-baseline{align-items:baseline}.row .column{display:block;flex:1 1 auto;margin-left:0;max-width:100%;width:100%}.row .column.column-offset-10{margin-left:10%}.row .column.column-offset-20{margin-left:20%}.row .column.column-offset-25{margin-left:25%}.row .column.column-offset-33,.row .column.column-offset-34{margin-left:33.3333%}.row .column.column-offset-50{margin-left:50%}.row .column.column-offset-66,.row .column.column-offset-67{margin-left:66.6666%}.row .column.column-offset-75{margin-left:75%}.row .column.column-offset-80{margin-left:80%}.row .column.column-offset-90{margin-left:90%}.row .column.column-10{flex:0 0 10%;max-width:10%}.row .column.column-20{flex:0 0 20%;max-width:20%}.row .column.column-25{flex:0 0 25%;max-width:25%}.row .column.column-33,.row .column.column-34{flex:0 0 33.3333%;max-width:33.3333%}.row .column.column-40{flex:0 0 40%;max-width:40%}.row .column.column-50{flex:0 0 50%;max-width:50%}.row .column.column-60{flex:0 0 60%;max-width:60%}.row .column.column-66,.row .column.column-67{flex:0 0 66.6666%;max-width:66.6666%}.row .column.column-75{flex:0 0 75%;max-width:75%}.row .column.column-80{flex:0 0 80%;max-width:80%}.row .column.column-90{flex:0 0 90%;max-width:90%}.row .column .column-top{align-self:flex-start}.row .column .column-bottom{align-self:flex-end}.row .column .column-center{-ms-grid-row-align:center;align-self:center}@media (min-width: 40rem){.row{flex-direction:row;margin-left:-1.0rem;width:calc(100% + 2.0rem)}.row .column{margin-bottom:inherit;padding:0 1.0rem}}a{color:#9b4dca;text-decoration:none}a:focus,a:hover{color:#606c76}dl,ol,ul{list-style:none;margin-top:0;padding-left:0}dl dl,dl ol,dl ul,ol dl,ol ol,ol ul,ul dl,ul ol,ul ul{font-size:90%;margin:1.5rem 0 1.5rem 3.0rem}ol{list-style:decimal inside}ul{list-style:circle inside}.button,button,dd,dt,li{margin-bottom:1.0rem}fieldset,input,select,textarea{margin-bottom:1.5rem}blockquote,dl,figure,form,ol,p,pre,table,ul{margin-bottom:2.5rem}table{border-spacing:0;width:100%}td,th{border-bottom:0.1rem solid #e1e1e1;padding:1.2rem 1.5rem;text-align:left}td:first-child,th:first-child{padding-left:0}td:last-child,th:last-child{padding-right:0}b,strong{font-weight:bold}p{margin-top:0}h1,h2,h3,h4,h5,h6{font-weight:300;letter-spacing:-.1rem;margin-bottom:2.0rem;margin-top:0}h1{font-size:4.6rem;line-height:1.2}h2{font-size:3.6rem;line-height:1.25}h3{font-size:2.8rem;line-height:1.3}h4{font-size:2.2rem;letter-spacing:-.08rem;line-height:1.35}h5{font-size:1.8rem;letter-spacing:-.05rem;line-height:1.5}h6{font-size:1.6rem;letter-spacing:0;line-height:1.4}img{max-width:100%}.clearfix:after{clear:both;content:' ';display:table}.float-left{float:left}.float-right{float:right}
			body {
				margin: 20px;
			}
			textarea {
				font-family: monospace;
				font-size: 14px;
				resize: vertical;
				height: 400px;
			}
			.result {
				background-color: #eee;
				border: 2px solid #ccc;
				padding: 8px;
				margin-bottom: 10px;
			}
		</style>
	</head>
	<body>
		<h1><a href="{{.Prefix}}">RDSS Archivematica Msgcreator</a></h1>
		<h3>Send a message to Kinesis</h3>
		<p>You can expect the message to be consumed by the RDSS Archivematica Channel Adapter.</p>
		<p>The sample <code>MetadataCreate</code> shown by us contains a couple of files that we know exist in the <code>{{.Bucket}}</code> sample bucket. You can choose a different bucket passing it in the URL, e.g. <a href="{{.Prefix}}with-files/{{.Bucket}}">/with-files/{{.Bucket}}</a>. The first 500 matches will be listed and included in the <code>files</code> list. You can add an extra prefix to filter the results, e.g.: <a href="{{.Prefix}}with-files/{{.Bucket}}/woodpigeon">/with-files/{{.Bucket}}/woodpigeon</a>.</p>
		{{if .Result}}
			<div class="result">
				{{.Result}}
				{{if .ShardID}}<br />ShardId: {{.ShardID}}{{end}}
				{{if .SequenceNumber}}<br />SequenceNumber: {{.SequenceNumber}}{{end}}
			</div>
		{{end}}
		<form method="POST">
			<textarea name="message">{{.DefaultMessage}}</textarea>
			<button type="submit" class="button">Send</a>
		</form>
	</body>
</html>
`

type Page struct {
	Prefix         string
	DefaultMessage string
	Bucket         string
	Result         string
	ShardID        string
	SequenceNumber string
}

var (
	tmpl = template.Must(template.New("index").Parse(html))
	re   = regexp.MustCompile("^/with-files/(.*)")
)

func handler(w http.ResponseWriter, r *http.Request) {
	log.Printf("Request received: method=%s path=%s", r.Method, r.URL)

	values := re.FindStringSubmatch(r.URL.Path)
	withFiles := len(values) > 1

	if r.URL.Path != "/" && !withFiles {
		http.Error(w, "", http.StatusNotFound)
		return
	}

	if r.Method == http.MethodPost {
		submitForm(w, r)
		return
	}

	if r.Method == http.MethodGet {
		if withFiles {
			renderFormWithFiles(w, r, values[1])
		} else {
			renderFormWithFiles(w, r, *s3DefaultBucket)
		}
		return
	}

	http.Error(w, "I don't know what you're trying to do!", http.StatusNotFound)
}

func renderFormWithFiles(w http.ResponseWriter, r *http.Request, query string) {
	parts := strings.Split(query, "/")

	var bucket, keyPrefix string
	if n := len(parts); n < 1 {
		http.Error(w, "", http.StatusNotFound)
		return
	} else if n == 1 {
		bucket = parts[0]
	} else if n > 1 {
		bucket = parts[0]
		keyPrefix = strings.Join(parts[1:], "/")
	}

	log.Printf("Accessing to S3, bucket=%s prefix=%s", bucket, keyPrefix)
	req := &s3.ListObjectsV2Input{
		Bucket:  aws.String(bucket),
		MaxKeys: aws.Int64(500),
		Prefix:  aws.String(keyPrefix),
	}
	resp, err := s3Client.ListObjectsV2(req)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error in S3 client: %s", err), http.StatusInternalServerError)
		return
	}

	message := &Message{
		Header: Header{
			ID:    uuid(),
			Class: "Command",
			Type:  "MetadataCreate",
		},
		Body: Body{
			Title: "Research about birds in the UK.",
			UUID:  uuid(),
		},
	}
	for index, object := range resp.Contents {
		message.Body.Files = append(message.Body.Files, &File{
			ID:          uuid(),
			Path:        fmt.Sprintf("s3://%s/%s", bucket, *object.Key),
			StorageType: "s3",
			Title:       fmt.Sprintf("Label of this intellectual asset: %d", index+1),
		})
	}
	msg, err := json.MarshalIndent(message, "", "  ")
	if err != nil {
		http.Error(w, fmt.Sprintf("Error encoding JSON: %s", err), http.StatusInternalServerError)
		return
	}
	renderForm(w, r, string(msg))
}

func submitForm(w http.ResponseWriter, r *http.Request) {
	p := &Page{Prefix: *prefix, Bucket: *s3DefaultBucket}
	if err := r.ParseForm(); err != nil {
		p.Result = fmt.Sprintf("The form could not be parsed: %s", err)
		renderTemplate(w, p)
		return
	}

	msg := r.PostFormValue("message")
	if msg == "" {
		p.Result = "The message is empty, try again!"
		renderTemplate(w, p)
		return
	}

	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Second*4)
	defer cancel()

	p.DefaultMessage = msg
	shardID, sequenceNumber, err := sendMessage(ctx, msg)
	if err != nil {
		p.Result = fmt.Sprintf("The message could not be sent: %s", err)
	} else {
		p.Result = "Message sent!"
		p.ShardID = shardID
		p.SequenceNumber = sequenceNumber
	}

	renderTemplate(w, p)
}

func renderForm(w http.ResponseWriter, r *http.Request, message string) {
	p := &Page{
		Prefix:         *prefix,
		DefaultMessage: message,
		Bucket:         *s3DefaultBucket,
	}

	renderTemplate(w, p)
}

func renderTemplate(w http.ResponseWriter, p *Page) {
	err := tmpl.Execute(w, p)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func sendMessage(ctx context.Context, msg string) (string, string, error) {
	blob := []byte(msg)

	err := json.Unmarshal(blob, &struct{}{})
	if err != nil {
		return "", "", err
	}

	req := &kinesis.PutRecordInput{
		Data:         blob,
		StreamName:   kinesisStream,
		PartitionKey: aws.String(strconv.FormatInt(time.Now().Unix(), 10)),
	}
	resp, err := kinesisClient.PutRecordWithContext(ctx, req)
	if err != nil {
		return "", "", err
	}

	return *resp.ShardId, *resp.SequenceNumber, err
}

var (
	kinesisClient   *kinesis.Kinesis
	kinesisStream   *string
	s3Client        *s3.S3
	s3DefaultBucket *string
	prefix          *string
)

func main() {
	var (
		addr            = flag.String("addr", "0.0.0.0:8000", "listen address")
		kinesisRegion   = flag.String("kinesis-region", "", "Kinesis - Region")
		kinesisEndpoint = flag.String("kinesis-endpoint", "", "Kinesis - Endpoint")
		s3AccessKey     = flag.String("s3-access-key", "", "S3 - Access key")
		s3SecretKey     = flag.String("s3-secret-key", "", "S3 - Secret key")
		s3Region        = flag.String("s3-region", "", "S3 - Region")
		s3Endpoint      = flag.String("s3-endpoint", "", "S3 - Endpoint")
	)
	prefix = flag.String("prefix", "/", "Path prefix, e.g.: `/msgcreator`, similar to `--prefix` in Jenkins")
	kinesisStream = flag.String("kinesis-stream", "main", "Kinesis - Stream")
	s3DefaultBucket = flag.String("s3-default-bucket", "rdss-prod-figshare-0132", "S3 - default bucket")
	flag.Parse()

	if !strings.HasSuffix(*prefix, "/") {
		*prefix += "/"
	}

	kinesisClient = getKinesisClient(kinesisRegion, kinesisEndpoint)
	s3Client = getS3Client(s3AccessKey, s3SecretKey, s3Region, s3Endpoint)

	log.Printf("HTTP server listening on http://%s", *addr)
	mux := http.NewServeMux()
	mux.HandleFunc("/", handler)
	http.ListenAndServe(*addr, mux)
}

func getKinesisClient(region, endpoint *string) *kinesis.Kinesis {
	config := aws.NewConfig()
	config.CredentialsChainVerboseErrors = aws.Bool(true)
	config.Credentials = credentials.NewStaticCredentials("foo", "bar", "")

	if *region != "" {
		config.Region = region
	}
	if *endpoint != "" {
		config.Endpoint = endpoint
	}

	sess := session.Must(session.NewSession(config))
	return kinesis.New(sess)
}

func getS3Client(accessKey, secretKey, region, endpoint *string) *s3.S3 {
	config := aws.NewConfig()
	config.CredentialsChainVerboseErrors = aws.Bool(true)
	config.Credentials = credentials.NewStaticCredentials(*accessKey, *secretKey, "")

	// Looking at minio...
	config.S3ForcePathStyle = aws.Bool(true)
	config.HTTPClient = &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}

	if *region != "" {
		config.Region = region
	}
	if *endpoint != "" {
		config.Endpoint = endpoint
	}

	sess := session.Must(session.NewSession(config))

	return s3.New(sess)
}

func uuid() (uuid string) {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		fmt.Println("Error: ", err)
		return
	}
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}
