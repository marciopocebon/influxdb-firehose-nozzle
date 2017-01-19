package main

import (
    "flag"
    "net/http"
    "encoding/json"
    "log"
    "time"
    "os"
    "strconv"
    "io"
 
    "github.com/cloudfoundry-community/go-cfclient"    
)

var (
	ApiAddress = flag.String(
		"api.address", "",
		"Cloud Foundry API Address ($API_ADDRESS).",
	)
        
	User = flag.String(
                "api.user", "", 
                "Cloud Foundry API User ($CF_USER).",
        )

	Password = flag.String(
                "api.password", "", 
                "Cloud Foundry API Password ($CF_PASSWORD).",
        )

	SkipSSL = flag.Bool(
                "skip.ssl", false, 
                "Disalbe SSL validation ($SKIP_SSL).",
        )
)

var request chan chan []AppInfo
var mapchan chan []AppInfo


type AppInfo struct {
        Name  string `json:"name"`
        Guid  string `json:"guid"`
        Space string `json:"space"`
        Org   string `json:"org"`
}

func overrideFlagsWithEnvVars() {
	overrideWithEnvVar("API_ADDRESS", ApiAddress)
	overrideWithEnvVar("CF_USER", User)
	overrideWithEnvVar("CF_PASSWORD", Password)
	overrideWithEnvBool("SKIP_SSL", SkipSSL)
}

func overrideWithEnvVar(name string, value *string) {
	envValue := os.Getenv(name)
	if envValue != "" {
		*value = envValue
	}
}

func overrideWithEnvUint(name string, value *uint) {
	envValue := os.Getenv(name)
	if envValue != "" {
		intValue, err := strconv.Atoi(envValue)
		if err != nil {
			log.Fatalln("Invalid `%s`: %s", name, err)
		}
		*value = uint(intValue)
	}
}

func overrideWithEnvBool(name string, value *bool) {
	envValue := os.Getenv(name)
	if envValue != "" {
		var err error
		*value, err = strconv.ParseBool(envValue)
		if err != nil {
			log.Fatalf("Invalid `%s`: %s", name, err)
		}
	}
}

func UpdateAppMap (client *cfclient.Client) {
   appmap := make([]AppInfo,0)

   go GenAppMap(client)  

   c := time.Tick(5 * time.Minute)
   for {
     select {
     case ch := <- request:
        ch <- appmap 
     case <-c:
	go GenAppMap(client)
     case freshmap := <- mapchan:
	appmap = freshmap 
	log.Printf("appmap updated")
     }
   }
}

func GenAppMap(client *cfclient.Client) {
	log.Println("generating fresh app map")
     	apps,err := client.ListApps()
	if err != nil {
		log.Printf("Error generating list of apps from CF: %v", err)
	}  

 	tempmap := make([]AppInfo,0)
 	var tempapp AppInfo 
 	for _, app := range apps {
		tempapp.Name=app.Name
		tempapp.Guid=app.Guid
		tempapp.Space=app.SpaceData.Entity.Name
		tempapp.Org=app.SpaceData.Entity.OrgData.Entity.Name
     
		tempmap = append(tempmap, tempapp)
	}
	mapchan <- tempmap
}

func main() {
  flag.Parse()
  overrideFlagsWithEnvVars()
 
  var port string
  request = make(chan chan []AppInfo)
  mapchan = make(chan []AppInfo)

  c := &cfclient.Config{
    ApiAddress:        *ApiAddress,
    Username:          *User,
    Password:          *Password,
    SkipSslValidation: *SkipSSL,
  }
  client, err := cfclient.NewClient(c)
  if err != nil {
	log.Fatal("Error connecting to API: %s", err.Error())
	os.Exit(1)
  }

 go UpdateAppMap(client)

 if port = os.Getenv("PORT"); len(port) == 0 {
        port = "8080" 
 }

 http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
	io.WriteString(w, "Cloud Foundry application lister.  Actual Information is served at /rest/apps/")
    })

 http.HandleFunc("/rest/apps/", func(w http.ResponseWriter, r *http.Request) {
        response := make(chan []AppInfo)
        request <- response
	appmapCopy := <- response
        json.NewEncoder(w).Encode(appmapCopy)
    })
 log.Fatal(http.ListenAndServe(":" + port, nil))
}
