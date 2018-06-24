package fr.edf.dco.ma.reflex

import java.util
import java.util.Properties

import akka.actor.{Actor, ActorLogging, ActorRef, Props, Stash}
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}

object ReflexKafkaConsumerActor {
  def props(topic: String, kafkaConfig: Properties, filterActor: ActorRef): Props = Props(new ReflexKafkaConsumerActor(topic, kafkaConfig, filterActor))

  //Le message indiquant que le prochain batch de messages doit être lu.
  case object PleasePoll

  case object StopWorking

  case object InitializationDone

}

class ReflexKafkaConsumerActor(topic: String, kafkaConfig: Properties, filterActor: ActorRef) extends Actor with ActorLogging {

  import ReflexKafkaConsumerActor._

  var kafkaConsumer: KafkaConsumer[String, String] = _

  override def preStart(): Unit = {
    super.preStart
    //Instanciation du consumer et souscription
    kafkaConsumer = new KafkaConsumer[String, String](kafkaConfig)
    kafkaConsumer.subscribe(util.Arrays.asList(topic))
  }

  //TODO: Cette approche ultra-simple ne survivra pas à plusieurs instances de l'acteur, et à autre chose que l'auto.commit.
  //TODO:
  def receive = {
    case PleasePoll => {
      log.info("Someone told me to poll !")
      //On prend le prochain groupe de messages et on l'envoie.
      val records: ConsumerRecords[String, String] = kafkaConsumer.poll(1000)
      val it = records.iterator()
      while (it.hasNext) {
        log.info("Found something to send !")
        filterActor ! it.next()
      }
      log.info("Done polling for now.")
    }
    case StopWorking => {
      kafkaConsumer.unsubscribe()
      kafkaConsumer.close
      log.info("Someone told me to shutdown !")
    }
  }
}
