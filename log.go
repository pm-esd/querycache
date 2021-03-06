package querycache

var Log Logger

func (configs *Configs) SetLogger(logger Logger) {
	Log = logger
}

type Logger interface {
	Panic(args ...interface{})
	Fatal(args ...interface{})
	Error(args ...interface{})
	Warning(args ...interface{})
	Warn(args ...interface{})
	Info(args ...interface{})
	Debug(args ...interface{})
	Trace(args ...interface{})
}

var trace bool

func (configs *Configs) SetTrace(t bool) {
	trace = t
}
