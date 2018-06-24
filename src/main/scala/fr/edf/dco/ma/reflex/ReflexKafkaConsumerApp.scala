package fr.edf.dco.ma.reflex

import java.util.Properties

import akka.actor.{ActorRef, ActorSystem}
import fr.edf.dco.ma.reflex.ReflexKafkaConsumerActor.{PleasePoll, StopWorking}
import info.batey.kafka.unit.KafkaUnit
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, OffsetResetStrategy}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

/**
  * Cet exécutable est une application de démonstration.
  */
object ReflexKafkaConsumerApp extends App {

  val system: ActorSystem = ActorSystem("SimpleKafkaConsumerAkkaApp")

  //Démarrage des services embarqués
  val kafkaUnitServer: KafkaUnit = new KafkaUnit(2181, 9092)
  kafkaUnitServer.startup

  //Ici on crée le topic kafka
  kafkaUnitServer.createTopic("test")

  var props = new Properties()
  props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUnitServer.getKafkaConnect)
  props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-actor-consumer")
  props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
  props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
  props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
  props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.LATEST.toString.toLowerCase)

  //Une fonction de filtrage qui laisse tout passer
  def filterFunction(c: ConsumerRecord[String, String]): Boolean = true

  //L'instanciation des acteurs
  val displayer: ActorRef = system.actorOf(ReflexKafkaDisplayActor.props, "displayActor")
  val filterer: ActorRef = system.actorOf(ReflexKafkaFilterActor.props(filterFunction, displayer), name = "filterActor")
  val consumer: ActorRef = system.actorOf(ReflexKafkaConsumerActor.props("test", props, filterer), name = "consumerActor")

  consumer ! PleasePoll

  Thread.sleep(1000)

  //Ici on injecte en synchrone des messages dans la file Kafka.
  val message: ProducerRecord[String, String] = new ProducerRecord[String, String]("test", "1", "Hello World !")
  var prodProps = new Properties()
  prodProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  prodProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  prodProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUnitServer.getKafkaConnect)
  val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](prodProps)
  producer.send(message).get

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
