package common

import (
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

var koVar map[string]string

func init() {
	koVar = make(map[string]string)
	for _, e := range os.Environ() {
		pair := strings.SplitN(e, "=", 2)
		// if pair[0] starts with KO_,
		if strings.HasPrefix(pair[0], "KO_") {
			koVar[pair[0]] = pair[1]
		}
	}
}

func SetVar(name, value string) {
	koVar[name] = value
}

func DelVar(name string) {
	delete(koVar, name)
}

func GetVar(name string) string {
	return koVar[name]
}

// GetValueOrEnv returns value itself, or if value is empty
// from koVar.
func GetValueOrEnv(value, name string) string {
	if value != "" {
		return value
	}
	return koVar[name]
}

func GetValueOrEnvInt(value, name string) (int, error) {
	v := GetValueOrEnv(value, name)
	return strconv.Atoi(v)
}

func SetLogLevel(loglv string) {
	switch loglv {
	case "debug":
		slog.SetLogLoggerLevel(slog.LevelDebug)
	case "info":
		slog.SetLogLoggerLevel(slog.LevelInfo)
	case "warn":
		slog.SetLogLoggerLevel(slog.LevelWarn)
	case "error":
		slog.SetLogLoggerLevel(slog.LevelError)
	default:
		log.Fatalf("Unknown log level: %s", loglv)
	}
}

var KoServer string
var KoServerPort int
var KoServerPath string
var KoAgent string
var KoAgentPort int
var KoAgentPath string
var KoDB string

func SetServer(server, port string, db string) {
	KoServer = GetValueOrEnv(server, "KO_SERVER")
	KoServerPort, _ = GetValueOrEnvInt(port, "KO_SERVER_PORT")
	KoDB = GetValueOrEnv(db, "KO_DB")
}

func SetAgent(agent, port string) {
	KoAgent = GetValueOrEnv(agent, "KO_AGENT")
	KoAgentPort, _ = GetValueOrEnvInt(port, "KO_AGENT_PORT")
}

func SetWorkingDir(wd string) {
	KoServerPath = filepath.Join(wd, "server")
	MustOK(os.MkdirAll(KoServerPath, 0755))

	KoAgentPath = filepath.Join(wd, "agent")
	MustOK(os.MkdirAll(KoAgentPath, 0755))
}
