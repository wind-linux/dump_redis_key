package internal

import (
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	lumberjack "gopkg.in/natefinch/lumberjack.v2"
)

/*************************
* author: Dev0026
* createTime: 18-11-17
* updateTime: 18-11-17
* description:
*************************/

var Logger *zap.Logger

func InitLoger(level zapcore.Level) {
	w := zap.CombineWriteSyncers(zapcore.AddSync(&lumberjack.Logger{
		Filename:   "./log.log",
		MaxSize:    15, // megabytes
		MaxBackups: 20,
		MaxAge:     28, // days
	}), zapcore.AddSync(os.Stdout))

	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(zapcore.EncoderConfig{
			TimeKey:        "ts",
			LevelKey:       "level",
			NameKey:        "logger",
			CallerKey:      "caller",
			MessageKey:     "msg",
			StacktraceKey:  "stacktrace",
			LineEnding:     zapcore.DefaultLineEnding,
			EncodeLevel:    zapcore.LowercaseLevelEncoder,
			EncodeTime:     zapcore.EpochNanosTimeEncoder,
			EncodeDuration: zapcore.SecondsDurationEncoder,
			EncodeCaller:   zapcore.ShortCallerEncoder,
		}),
		w,
		level,
	)
	Logger = zap.New(core)

	Info("init zap log SUCCESS")
}

// Debug logs a message at DebugLevel. The message includes any fields passed
// at the log site, as well as any fields accumulated on the logger.
func Debug(msg string, fields ...zap.Field) {
	Logger.Debug(msg, fields...)
}

// Info logs a message at InfoLevel. The message includes any fields passed
// at the log site, as well as any fields accumulated on the logger.
func Info(msg string, fields ...zap.Field) {
	Logger.Info(msg, fields...)
}

// Warn logs a message at WarnLevel. The message includes any fields passed
// at the log site, as well as any fields accumulated on the logger.
func Warn(msg string, fields ...zap.Field) {
	Logger.Warn(msg, fields...)
}

// Error logs a message at ErrorLevel. The message includes any fields passed
// at the log site, as well as any fields accumulated on the logger.
func Error(msg string, fields ...zap.Field) {
	Logger.Error(msg, fields...)
}

// DPanic logs a message at DPanicLevel. The message includes any fields
// passed at the log site, as well as any fields accumulated on the logger.
//
// If the logger is in development mode, it then panics (DPanic means
// "development panic"). This is useful for catching errors that are
// recoverable, but shouldn't ever happen.
func DPanic(msg string, fields ...zap.Field) {
	Logger.DPanic(msg, fields...)
}

// Panic logs a message at PanicLevel. The message includes any fields passed
// at the log site, as well as any fields accumulated on the logger.
//
// The logger then panics, even if logging at PanicLevel is disabled.
func Panic(msg string, fields ...zap.Field) {
	Logger.Panic(msg, fields...)
}

// Fatal logs a message at FatalLevel. The message includes any fields passed
// at the log site, as well as any fields accumulated on the logger.
//
// The logger then calls os.Exit(1), even if logging at FatalLevel is
// disabled.
func Fatal(msg string, fields ...zap.Field) {
	Logger.Fatal(msg, fields...)
}
