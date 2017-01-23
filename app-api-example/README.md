## Summary

Example application to enumerate all apps in a cloudfoundry enviornment and export their metadata via json using the [cf go-client] (https://github.com/cloudfoundry-community/go-cfclient).

## CF Setup

This application requires a UAA user with administrative access so it can enumerate cf applications across all orgs and spaces.  At a minimum you should configure the following parameters, either via enviornment variables or command line parameters:
```
  API_ADDRESS: https://api.cf.company.com
  CF_USER: admin
  SKIP_SSL: true
  CF_PASSWORD: secret
```

## Running

The application will listen on two endpoints:
* /  - root path to give basic app information and respond to diego healthchecks 

* /rest/apps/ - exposes information about all applications in the cloudfoundry enviornment in the following json format.

```
[{
  "name": "app1",
  "guid": "d5697f98-1a94-4d92-a93b-6ba812c9f67a",
  "space": "testing",
  "org": "myorg"
}, {
  "name": "app2",
  "guid": "164686ca-0c4f-42b7-95d3-3b1ee0fb095c",
  "space": "testing",
  "org": "myorg"
}]
```
