package influxdbclient

import (
	"bytes"
	"crypto/sha1"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cloudfoundry/gosteno"
	"github.com/cloudfoundry/sonde-go/events"
	"github.com/evoila/influxdb-firehose-nozzle/cfinstanceinfoapi"
)

type Client struct {
	url                   string
	database              string
	user                  string
	password              string
	allowSelfSigned       bool
	metricPoints          map[metricKey]metricValue
	prefix                string
	deployment            string
	ip                    string
	tagsHash              string
	totalMessagesReceived uint64
	totalMetricsSent      uint64
	log                   *gosteno.Logger
	appinfo               map[string]cfinstanceinfoapi.AppInfo
}

type metricKey struct {
	eventType events.Envelope_EventType
	name      string
	tagsHash  string
}

type metricValue struct {
	tags   []string
	points []Point
}

type Metric struct {
	Metric string   `json:"metric"`
	Points []Point  `json:"points"`
	Type   string   `json:"type"`
	Host   string   `json:"host,omitempty"`
	Tags   []string `json:"tags,omitempty"`
}

type Point struct {
	Timestamp int64
	Value     float64
}

func New(url string, database string, user string, password string, allowSelfSigned bool, prefix string, deployment string, ip string, log *gosteno.Logger, appinfo map[string]cfinstanceinfoapi.AppInfo) *Client {
	ourTags := []string{
		"deployment:" + deployment,
		"ip:" + ip,
	}
	return &Client{
		url:             url,
		database:        database,
		user:            user,
		password:        password,
		allowSelfSigned: allowSelfSigned,
		metricPoints:    make(map[metricKey]metricValue),
		prefix:          prefix,
		deployment:      deployment,
		ip:              ip,
		log:             log,
		tagsHash:        hashTags(ourTags),
		appinfo:         appinfo,
	}
}

func (c *Client) AlertSlowConsumerError() {
	c.addInternalMetric("slowConsumerAlert", uint64(1))
}

func (c *Client) AddMetric(envelope *events.Envelope) {
	c.totalMessagesReceived++

	switch envelope.GetEventType() {
	case events.Envelope_CounterEvent:
		c.AddCounterValueMetric(envelope)
	case events.Envelope_ValueMetric:
		c.AddCounterValueMetric(envelope)
	case events.Envelope_ContainerMetric:
		c.AddContainerMetric(envelope)
	case events.Envelope_HttpStartStop:
		c.AddHttpStartStopMetric(envelope)
	}
}

func (c *Client) AddHttpStartStopMetric(envelope *events.Envelope) {
	if envelope.GetHttpStartStop().GetApplicationId() != nil {
		tags := parseTags(envelope, c)
		types := []string{"httprequest.duration_ms", "httprequest.size_bytes"}

		for v := range types {
			switch types[v] {
			case "httprequest.duration_ms":
				key := metricKey{
					eventType: envelope.GetEventType(),
					name:      envelope.GetOrigin() + ".container_" + types[v],
					tagsHash:  hashTags(tags),
				}
				mVal := c.metricPoints[key]
				value := (envelope.GetHttpStartStop().GetStopTimestamp() - envelope.GetHttpStartStop().GetStartTimestamp()) / 1e6
				if value < 0 {
					value = 0
				}
				mVal.tags = tags
				mVal.points = append(mVal.points, Point{
					Timestamp: envelope.GetTimestamp() / int64(time.Second),
					Value:     float64(value),
				})
				c.metricPoints[key] = mVal
			case "httprequest.size_bytes":
				key := metricKey{
					eventType: envelope.GetEventType(),
					name:      envelope.GetOrigin() + ".container_" + types[v],
					tagsHash:  hashTags(tags),
				}
				mVal := c.metricPoints[key]
				value := envelope.GetHttpStartStop().GetContentLength()
				if value < 0 {
					value = 0
				}
				mVal.tags = tags
				mVal.points = append(mVal.points, Point{
					Timestamp: envelope.GetTimestamp() / int64(time.Second),
					Value:     float64(value),
				})
				c.metricPoints[key] = mVal
			}
		}
	}
}

func (c *Client) AddContainerMetric(envelope *events.Envelope) {
	types := []string{"cpu", "mem_bytes", "mem_bytes_quota", "disk_bytes", "disk_bytes_quota"}
	tags := parseTags(envelope, c)

	for v := range types {
		switch types[v] {
		case "cpu":
			key := metricKey{
				eventType: envelope.GetEventType(),
				name:      envelope.GetOrigin() + ".container_" + types[v],
				tagsHash:  hashTags(tags),
			}
			mVal := c.metricPoints[key]
			value := envelope.GetContainerMetric().GetCpuPercentage()

			mVal.tags = tags
			mVal.points = append(mVal.points, Point{
				Timestamp: envelope.GetTimestamp() / int64(time.Second),
				Value:     value,
			})
			c.metricPoints[key] = mVal
		case "mem_bytes":
			key := metricKey{
				eventType: envelope.GetEventType(),
				name:      envelope.GetOrigin() + ".container_" + types[v],
				tagsHash:  hashTags(tags),
			}
			mVal := c.metricPoints[key]
			value := envelope.GetContainerMetric().GetMemoryBytes()

			mVal.tags = tags
			mVal.points = append(mVal.points, Point{
				Timestamp: envelope.GetTimestamp() / int64(time.Second),
				Value:     float64(value),
			})
			c.metricPoints[key] = mVal
		case "mem_bytes_quota":
			key := metricKey{
				eventType: envelope.GetEventType(),
				name:      envelope.GetOrigin() + ".container_" + types[v],
				tagsHash:  hashTags(tags),
			}
			mVal := c.metricPoints[key]
			value := envelope.GetContainerMetric().GetMemoryBytesQuota()

			mVal.tags = tags
			mVal.points = append(mVal.points, Point{
				Timestamp: envelope.GetTimestamp() / int64(time.Second),
				Value:     float64(value),
			})
			c.metricPoints[key] = mVal
		case "disk_bytes":
			key := metricKey{
				eventType: envelope.GetEventType(),
				name:      envelope.GetOrigin() + ".container_" + types[v],
				tagsHash:  hashTags(tags),
			}
			mVal := c.metricPoints[key]
			value := envelope.GetContainerMetric().GetDiskBytes()

			mVal.tags = tags
			mVal.points = append(mVal.points, Point{
				Timestamp: envelope.GetTimestamp() / int64(time.Second),
				Value:     float64(value),
			})
			c.metricPoints[key] = mVal
		case "disk_bytes_quota":
			key := metricKey{
				eventType: envelope.GetEventType(),
				name:      envelope.GetOrigin() + ".container_" + types[v],
				tagsHash:  hashTags(tags),
			}
			mVal := c.metricPoints[key]
			value := envelope.GetContainerMetric().GetDiskBytesQuota()

			mVal.tags = tags
			mVal.points = append(mVal.points, Point{
				Timestamp: envelope.GetTimestamp() / int64(time.Second),
				Value:     float64(value),
			})
			c.metricPoints[key] = mVal
		}
	}
}

func (c *Client) AddCounterValueMetric(envelope *events.Envelope) {
	tags := parseTags(envelope, c)
	key := metricKey{
		eventType: envelope.GetEventType(),
		name:      getName(envelope),
		tagsHash:  hashTags(tags),
	}

	mVal := c.metricPoints[key]
	value := getValue(envelope)

	mVal.tags = tags
	mVal.points = append(mVal.points, Point{
		Timestamp: envelope.GetTimestamp() / int64(time.Second),
		Value:     value,
	})

	c.metricPoints[key] = mVal
}

func (c *Client) PostMetrics() error {
	url := c.seriesURL()

	c.populateInternalMetrics()
	numMetrics := len(c.metricPoints)
	c.log.Infof("Posting %d metrics", numMetrics)

	seriesBytes, metricsCount := c.formatMetrics()
	c.log.Infof("Posting data %s", seriesBytes)

	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	httpClient := &http.Client{Transport: tr}

	resp, err := httpClient.Post(url, "application/binary", bytes.NewBuffer(seriesBytes))
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	if resp.StatusCode >= 300 || resp.StatusCode < 200 {
		return fmt.Errorf("InfluxDB request returned HTTP response: %s", resp.Status)
	}

	c.totalMetricsSent += metricsCount
	c.metricPoints = make(map[metricKey]metricValue)

	return nil
}

func (c *Client) seriesURL() string {
	url := fmt.Sprintf("%s/write?db=%s&precision=s", c.url, c.database)
	c.log.Infof("Using the following influx URL " + url)
	return url
}

func (c *Client) populateInternalMetrics() {
	c.addInternalMetric("totalMessagesReceived", c.totalMessagesReceived)
	c.addInternalMetric("totalMetricsSent", c.totalMetricsSent)

	if !c.containsSlowConsumerAlert() {
		c.addInternalMetric("slowConsumerAlert", uint64(0))
	}
}

func (c *Client) containsSlowConsumerAlert() bool {
	key := metricKey{
		name:     "slowConsumerAlert",
		tagsHash: c.tagsHash,
	}
	_, ok := c.metricPoints[key]
	return ok
}

func (c *Client) formatMetrics() ([]byte, uint64) {
	var buffer bytes.Buffer

	for key, mVal := range c.metricPoints {
		buffer.WriteString(c.prefix + key.name)
		buffer.WriteString(",")
		buffer.WriteString(formatTags(mVal.tags))
		buffer.WriteString(" ")
		buffer.WriteString(formatValues(mVal.points))
		buffer.WriteString(" ")
		buffer.WriteString(formatTimestamp(mVal.points))
		buffer.WriteString("\n")
	}

	return buffer.Bytes(), uint64(len(c.metricPoints))
}

func formatTags(tags []string) string {
	var newTags string
	for index, tag := range tags {
		if index > 0 {
			newTags += ","
		}

		newTags += tag
	}
	return newTags
}

func formatValues(points []Point) string {
	var newPoints string
	for index, point := range points {
		if index > 0 {
			newPoints += ","
		}

		newPoints += "value=" + strconv.FormatFloat(point.Value, 'f', -1, 64)
	}
	return newPoints
}

func formatTimestamp(points []Point) string {
	if len(points) > 0 {
		return strconv.FormatInt(points[0].Timestamp, 10)
	} else {
		return strconv.FormatInt(time.Now().Unix(), 10)
	}
}

func (c *Client) addInternalMetric(name string, value uint64) {
	key := metricKey{
		name:     name,
		tagsHash: c.tagsHash,
	}

	point := Point{
		Timestamp: time.Now().Unix(),
		Value:     float64(value),
	}

	mValue := metricValue{
		tags: []string{
			fmt.Sprintf("ip=%s", c.ip),
			fmt.Sprintf("deployment=%s", c.deployment),
		},
		points: []Point{point},
	}

	c.metricPoints[key] = mValue
}

func getName(envelope *events.Envelope) string {
	switch envelope.GetEventType() {
	case events.Envelope_ValueMetric:
		return envelope.GetOrigin() + "." + envelope.GetValueMetric().GetName()
	case events.Envelope_CounterEvent:
		return envelope.GetOrigin() + "." + envelope.GetCounterEvent().GetName()
	default:
		panic("Unknown event type")
	}
}

func getValue(envelope *events.Envelope) float64 {
	switch envelope.GetEventType() {
	case events.Envelope_ValueMetric:
		return envelope.GetValueMetric().GetValue()
	case events.Envelope_CounterEvent:
		return float64(envelope.GetCounterEvent().GetTotal())
	default:
		panic("Unknown event type")
	}
}

func parseTags(envelope *events.Envelope, c *Client) []string {
	tags := appendTagIfNotEmpty(nil, "deployment", envelope.GetDeployment())

	switch envelope.GetEventType() {
	case events.Envelope_ValueMetric:
		tags = appendTagIfNotEmpty(tags, "job", envelope.GetJob())
		tags = appendTagIfNotEmpty(tags, "index", envelope.GetIndex())
		tags = appendTagIfNotEmpty(tags, "ip", envelope.GetIp())
	case events.Envelope_CounterEvent:
		tags = appendTagIfNotEmpty(tags, "job", envelope.GetJob())
		tags = appendTagIfNotEmpty(tags, "index", envelope.GetIndex())
		tags = appendTagIfNotEmpty(tags, "ip", envelope.GetIp())
	case events.Envelope_ContainerMetric:
		tags = appendTagIfNotEmpty(tags, "appguid", envelope.GetContainerMetric().GetApplicationId())
		tags = appendTagIfNotEmpty(tags, "index", strconv.FormatInt(int64(envelope.GetContainerMetric().GetInstanceIndex()), 10))
		tags = appendTagIfNotEmpty(tags, "appname", c.appinfo[envelope.GetContainerMetric().GetApplicationId()].Name)
		tags = appendTagIfNotEmpty(tags, "org", c.appinfo[envelope.GetContainerMetric().GetApplicationId()].Org)
		tags = appendTagIfNotEmpty(tags, "space", c.appinfo[envelope.GetContainerMetric().GetApplicationId()].Space)
	case events.Envelope_HttpStartStop:
		tags = appendTagIfNotEmpty(tags, "appguid", UUIDToString(envelope.GetHttpStartStop().GetApplicationId()))
		//always 0
		//tags = appendTagIfNotEmpty(tags, "index", strconv.FormatInt(int64(envelope.GetHttpStartStop().GetInstanceIndex()), 10))
		tags = appendTagIfNotEmpty(tags, "statuscode", strconv.FormatInt(int64(envelope.GetHttpStartStop().GetStatusCode()), 10))
		//take out anything after a whitespace
		prettyuri := strings.Fields(envelope.GetHttpStartStop().GetUri())
		tags = appendTagIfNotEmpty(tags, "uri", prettyuri[0])
		//always the loadbalancer in front of CF so not useful
		//tags = appendTagIfNotEmpty(tags, "remoteaddress", envelope.GetHttpStartStop().GetRemoteAddress())
		tags = appendTagIfNotEmpty(tags, "appname", c.appinfo[UUIDToString(envelope.GetHttpStartStop().GetApplicationId())].Name)
		tags = appendTagIfNotEmpty(tags, "org", c.appinfo[UUIDToString(envelope.GetHttpStartStop().GetApplicationId())].Org)
		tags = appendTagIfNotEmpty(tags, "space", c.appinfo[UUIDToString(envelope.GetHttpStartStop().GetApplicationId())].Space)
	}

	for tname, tvalue := range envelope.GetTags() {
		tags = appendTagIfNotEmpty(tags, tname, tvalue)
	}

	return tags
}

func appendTagIfNotEmpty(tags []string, key, value string) []string {
	if value != "" {
		tags = append(tags, fmt.Sprintf("%s=%s", key, value))
	}
	return tags
}

func hashTags(tags []string) string {
	sort.Strings(tags)
	hash := ""
	for _, tag := range tags {
		tagHash := sha1.Sum([]byte(tag))
		hash += string(tagHash[:])
	}
	return hash
}

func UUIDToString(uuid *events.UUID) string {
	var uuidBytes [16]byte

	if uuid == nil {
		return ""
	}

	binary.LittleEndian.PutUint64(uuidBytes[:8], uuid.GetLow())
	binary.LittleEndian.PutUint64(uuidBytes[8:], uuid.GetHigh())

	return fmt.Sprintf("%x-%x-%x-%x-%x", uuidBytes[0:4], uuidBytes[4:6], uuidBytes[6:8], uuidBytes[8:10], uuidBytes[10:])
}
