package fr.edf.dco.ma.reflex

import java.util
import java.util.Properties

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import com.typesafe.config.Config
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}

object ReflexKafkaConsumerActor {
  def props(topic: String, kafkaConfig: Properties, filterActor: ActorRef): Props = Props(new ReflexKafkaConsumerActor(topic, kafkaConfig, filterActor))
  //Le message indiquant que le prochain batch de messages doit être lu.
  case object GetMeNextBatch
}

class ReflexKafkaConsumerActor(topic: String, kafkaConfig: Properties, filterActor: ActorRef) extends Actor with ActorLogging {
  import ReflexKafkaConsumerActor._

  //Instanciation du consumer et souscription
  var kafkaConsumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](kafkaConfig)
  kafkaConsumer.subscribe(util.Arrays.asList(topic))

  //TODO: Implémenter la gestion fine de l'offset.
  def receive = {
    case GetMeNextBatch => {
      //On prend le prochain groupe de messages et on l'envoie.
      val records: ConsumerRecords[String, String] = kafkaConsumer.poll(Long.MaxValue)
      val it = records.iterator()
      while (it.hasNext) {
        filterActor ! it.next()
      }
    }
  }
}
