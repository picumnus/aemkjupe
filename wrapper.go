package aemkjupe

import (
	"github.com/streadway/amqp"
	"go.uber.org/zap"
	"time"
)

const name = "aemkjupe"

type Amqp struct {
	log         *zap.SugaredLogger
	connstr     string
	qCfg        QueueConfig
	exCfg       *ExchangeConfig
	MsgChan     chan []byte
	closeConn   chan *amqp.Error
	closeCh     chan *amqp.Error
	stopCh      chan bool
	isPublisher bool
}

type ExchangeConfig struct {
	Name       string
	Type       string
	Durable    bool
	AutoDelete bool
	Passive    bool
}

type QueueConfig struct {
	Name      string
	Durable   bool
	Exclusive bool
}

func (a *Amqp) publisher(conn *amqp.Connection, ch *amqp.Channel) {
	defer func() {
		if r := recover(); r != nil {
			a.log.Error("PANIC DETECTED :%+v", r)
		}
	}()
	a.log.Debugf("%s: starting publisher", name)
	conn.NotifyClose(a.closeConn)
	conn.NotifyClose(a.closeCh)
	exName := ""
	if a.exCfg != nil {
		exName = a.exCfg.Name
	}
	for {
		select {
		case msg := <-a.MsgChan:
			err := ch.Publish(exName, a.qCfg.Name, false, false, amqp.Publishing{
				Expiration:  "60000",
				ContentType: "text/plain",
				Body:        msg,
			})
			if err != nil {
				a.log.Errorf("%s: failed to publish msg: %+v", name, err)
			} else {
				a.log.Debugf("SENT: %s", string(msg))
			}
		case err := <-a.closeCh:
			if err != nil {
				a.log.Infof("%s: close received info: %d, %s", name, err.Code, err.Reason)
			} else {
				a.log.Infof("%s: closed received", name)
			}

			time.Sleep(3 * time.Second)
			go a.Run()
			return
		case err := <-a.closeConn:
			if err != nil {
				a.log.Infof("%s: close received info: %d, %s", name, err.Code, err.Reason)
			} else {
				a.log.Infof("%s: closed received", name)
			}

			time.Sleep(3 * time.Second)
			go a.Run()
			return
		}
	}
}

func (a *Amqp) consumer(conn *amqp.Connection, msgs <-chan amqp.Delivery) {
	defer func() {
		if r := recover(); r != nil {
			a.log.Error("%s: PANIC DETECTED :%+v", name, r)
		}
	}()
	a.log.Debugf("%s: starting consumer", name)
	conn.NotifyClose(a.closeConn)
	conn.NotifyClose(a.closeCh)
	for {
		select {
		case msg := <-msgs:
			a.MsgChan <- msg.Body
		case err := <-a.closeCh:
			if err != nil {
				a.log.Infof("%s: close received info: %d, %s", name, err.Code, err.Reason)
			} else {
				a.log.Infof("%s: closed received", name)
			}
			time.Sleep(3 * time.Second)
			go a.Run()
			return
		case err := <-a.closeConn:
			if err != nil {
				a.log.Infof("%s: close received info: %d, %s", name, err.Code, err.Reason)
			} else {
				a.log.Infof("%s: closed received", name)
			}
			time.Sleep(3 * time.Second)
			go a.Run()
			return
		case <-a.stopCh:
			a.log.Debugf("%s: stop", name)
			err := conn.Close()
			if err != nil {
				a.log.Errorf("%s: %+v", name, err)
			}
			return
		}
	}
}

func (a *Amqp) connect() error {
	defer func() {
		if r := recover(); r != nil {
			a.log.Error("%s: PANIC DETECTED: %+v", name, r)
		}
	}()
	a.log.Debugf("%s: init connection", name)
	conn, err := amqp.Dial(a.connstr)
	if err != nil {
		return err
	}
	a.log.Debugf("%s: dial ok", name)
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	a.log.Debugf("%s: channel ok", name)
	exName := ""
	if a.exCfg != nil {
		exName = a.exCfg.Name
		if a.exCfg.Passive {
			err = ch.ExchangeDeclarePassive(a.exCfg.Name, a.exCfg.Type, a.exCfg.Durable, a.exCfg.AutoDelete, false, false, nil)
		} else {
			err = ch.ExchangeDeclare(a.exCfg.Name, a.exCfg.Type, a.exCfg.Durable, a.exCfg.AutoDelete, false, false, nil)
		}
		if err != nil {
			return err
		}
		a.log.Debugf("%s: exchange ok", name)
	}
	q, err := ch.QueueDeclare(a.qCfg.Name, a.qCfg.Durable, false, a.qCfg.Exclusive, false, nil)
	if err != nil {
		return err
	}
	a.log.Debugf("%s: queue declare ok", name)
	if a.isPublisher {
		go a.publisher(conn, ch)
		return nil
	}

	err = ch.QueueBind(q.Name, "", exName, false, nil)
	if err != nil {
		return err
	}
	a.log.Debugf("%s: queue bind ok", name)
	msgs, err := ch.Consume(q.Name, "", true, false, false, false, nil)
	if err != nil {
		return err
	}
	a.log.Debugf("%s: channel consume ok", name)
	go a.consumer(conn, msgs)
	return nil
}

func (a *Amqp) Run() {
	a.closeCh = make(chan *amqp.Error)
	a.closeConn = make(chan *amqp.Error)
	recSecsInterval := 3
	for {
		a.log.Infof("%s: connecting", name)
		err := a.connect()
		if err != nil {
			a.log.Errorf("%s: %+v", name, err)
			a.log.Infof("%s: waiting %d seconds before reconnecting", name, recSecsInterval)
			time.Sleep(time.Second * time.Duration(recSecsInterval))
		} else {
			break
		}
	}
}

func NewPublisher(connstr string, qc QueueConfig, ex *ExchangeConfig, log *zap.SugaredLogger) *Amqp {
	if log == nil {
		log = logger()
	}
	return &Amqp{
		log:         log,
		connstr:     connstr,
		qCfg:        qc,
		exCfg:       ex,
		isPublisher: true,
		MsgChan:     make(chan []byte, 512),
		closeConn:   make(chan *amqp.Error),
		stopCh:      make(chan bool),
	}
}

func NewConsumer(connstr string, qc QueueConfig, ex *ExchangeConfig, log *zap.SugaredLogger) *Amqp {
	if log == nil {
		log = logger()
	}
	return &Amqp{
		log:         log,
		connstr:     connstr,
		qCfg:        qc,
		exCfg:       ex,
		isPublisher: false,
		MsgChan:     make(chan []byte, 4096),
		closeConn:   make(chan *amqp.Error),
		closeCh:     make(chan *amqp.Error),
		stopCh:      make(chan bool),
	}
}

func logger() *zap.SugaredLogger {
	logCfg := zap.NewProductionConfig()
	logCfg.Level.SetLevel(zap.PanicLevel)
	logger, _ := logCfg.Build()

	return logger.Sugar()
}
