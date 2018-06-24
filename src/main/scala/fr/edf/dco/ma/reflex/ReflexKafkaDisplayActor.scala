package fr.edf.dco.ma.reflex

import akka.actor.{Actor, ActorLogging, Props}
import org.apache.kafka.clients.consumer.ConsumerRecord

object ReflexKafkaDisplayActor {
  def props: Props = Props[ReflexKafkaDisplayActor]
}

class ReflexKafkaDisplayActor extends Actor with ActorLogging {

  override def receive: Receive = {
    case r: ConsumerRecord[String, String] => log.info(s"Message reçu : ${r.value()}")
  }
}
