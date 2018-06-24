package fr.edf.dco.ma.reflex

import java.util.Properties

import akka.actor.{ActorRef, ActorSystem}
import fr.edf.dco.ma.reflex.ReflexKafkaConsumerActor.{Initialize, PleasePoll, StopWorking}
import info.batey.kafka.unit.KafkaUnit
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

object ReflexKafkaConsumerApp extends App {

  //Ici on crée le topic kafka
  val kafkaUnitServer: KafkaUnit = new KafkaUnit(2181, 9092)
  kafkaUnitServer.startup
  kafkaUnitServer.createTopic("test")

  val props = new Properties()
  props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUnitServer.getKafkaConnect)
  props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-actor-consumer")
  props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
  props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
  props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
  props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  val system: ActorSystem = ActorSystem("SimpleKafkaConsumerAkkaApp")

  def filterFunction(c: ConsumerRecord[String, String]): Boolean = true

  val displayer: ActorRef = system.actorOf(ReflexKafkaDisplayActor.props, "displayActor")
  val filterer: ActorRef = system.actorOf(ReflexKafkaFilterActor.props(filterFunction, displayer), name = "filterActor")
  val consumer: ActorRef = system.actorOf(ReflexKafkaConsumerActor.props("test", props ,filterer), name="consumerActor")

  consumer ! Initialize
  Thread.sleep(1000)

  //On déclenche une lecture
  consumer ! PleasePoll

  //Ici on injecte des messages dans la file Kafka.
  val message: ProducerRecord[String, String] = new ProducerRecord[String, String]("test", "1", "Hello World !")
  val prodProps = new Properties()
  prodProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  prodProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  prodProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUnitServer.getKafkaConnect)
  val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](prodProps)
  producer.send(message).get


  //On déclenche une lecture
  consumer ! PleasePoll

  Thread.sleep(2000)

  //On dit à l'acteur d'arrêter
  consumer ! StopWorking

  Thread.sleep(2000)

  producer.close
  kafkaUnitServer.shutdown
  System.out.println("Do we even reach here ?")
}
