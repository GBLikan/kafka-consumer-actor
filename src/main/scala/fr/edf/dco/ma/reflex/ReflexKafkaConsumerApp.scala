package fr.edf.dco.ma.reflex

import java.util.Properties

import akka.actor.{ActorRef, ActorSystem}
import fr.edf.dco.ma.reflex.ReflexKafkaConsumerActor.{PleasePoll, StopWorking}
import info.batey.kafka.unit.KafkaUnit
import org.apache.kafka.clients.consumer._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import scala.util.Random

/**
  * Cet exécutable est une application de démonstration et de POC.
  * Utiliser plutôt ReflexConsumerSpec pour industrialiser les tests de comportement.
  */
object ReflexKafkaConsumerApp extends App {

  val system: ActorSystem = ActorSystem("SimpleKafkaConsumerAkkaApp")
  val randomString: String = Random.alphanumeric.take(5).mkString("")


  //Démarrage des services embarqués
  val kafkaUnitServer: KafkaUnit = new KafkaUnit(2181, 9092)
  kafkaUnitServer.startup

  //Ici on crée le topic kafka
  kafkaUnitServer.createTopic("test")

  var props = new Properties()
  props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ConsumerConfig.GROUP_ID_CONFIG, randomString)
  props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
  props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
  props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
  props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
  props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.EARLIEST.toString.toLowerCase)

  //Une fonction de filtrage qui laisse tout passer
  def filterFunction(c: ConsumerRecord[String, String]): Boolean = true

  //L'instanciation des acteurs
  val displayer: ActorRef = system.actorOf(ReflexKafkaDisplayActor.props, "displayActor")
  val filterer: ActorRef = system.actorOf(ReflexKafkaFilterActor.props(filterFunction, displayer), name = "filterActor")
  val consumer: ActorRef = system.actorOf(ReflexKafkaConsumerActor.props("test", props, filterer), name = "consumerActor")

  consumer ! PleasePoll

  Thread.sleep(1000)

  //Ici on injecte en synchrone des messages dans la file Kafka.
  val message: ProducerRecord[String, String] = new ProducerRecord[String, String]("test", "1", "Hello World !" + randomString)
  var prodProps = new Properties()
  prodProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  prodProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  prodProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](prodProps)
  producer.send(message).get
  System.out.println("Sent Hello World !" + randomString)

  //On déclenche une lecture
  consumer ! PleasePoll

  Thread.sleep(1000)

  //On dit à l'acteur d'arrêter
  consumer ! StopWorking

  Thread.sleep(1000)

  //On termine les services et l'ActorSystem
  producer.close
  kafkaUnitServer.shutdown
  system.terminate
}
