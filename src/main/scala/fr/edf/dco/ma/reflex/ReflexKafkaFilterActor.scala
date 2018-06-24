package fr.edf.dco.ma.reflex

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import org.apache.kafka.clients.consumer.ConsumerRecord

object ReflexKafkaFilterActor {
  def props(filterFunction: ConsumerRecord[String, String] => Boolean, processorActor: ActorRef): Props = Props(new ReflexKafkaFilterActor(filterFunction, processorActor))

}

class ReflexKafkaFilterActor(filterFunction: ConsumerRecord[String, String] => Boolean, processorActor: ActorRef) extends Actor with ActorLogging {

  override def receive: Receive = {
    case r: ConsumerRecord[String, String] => if (filterFunction(r)) processorActor ! r else log.debug(s"Message ${r.value()} discarded")
  }
}
