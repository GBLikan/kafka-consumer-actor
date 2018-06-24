package fr.edf.dco.ma.reflex

import java.util.Properties

import akka.actor.{ActorRef, ActorSystem}
import fr.edf.dco.ma.reflex.ReflexKafkaConsumerActor.GetMeNextBatch
import info.batey.kafka.unit.KafkaUnit
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

object ReflexKafkaConsumerApp extends App {

  //Ici on crée le topic kafka
  val kafkaUnitServer: KafkaUnit = new KafkaUnit("localhost:5000", "localhost:5001")
  kafkaUnitServer.createTopic("test")

  val props = new Properties()
  props.put("bootstrap.servers", kafkaUnitServer.getKafkaConnect)
  props.put("group.id", "test-actor-consumer")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

  val system: ActorSystem = ActorSystem("SimpleKafkaConsumerAkkaApp")

  def filterFunction(c: ConsumerRecord[String, String]): Boolean = true

  val displayer: ActorRef = system.actorOf(ReflexKafkaDisplayActor.props, "displayActor")
  val filterer: ActorRef = system.actorOf(ReflexKafkaFilterActor.props(filterFunction, displayer), name = "filterActor")
  val consumer: ActorRef = system.actorOf(ReflexKafkaConsumerActor.props("test", props ,filterer), name="consumerActor")

  //Ici on injecte des messages dans la file Kafka.
  val message: ProducerRecord[String, String] = new ProducerRecord[String, String]("test", "1", "Hello World !")
  val prodProps = new Properties()
  prodProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
  prodProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
  prodProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUnitServer.getKafkaConnect)
  val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](prodProps)
  producer.send(message).get


  //On déclenche une lecture
  consumer ! GetMeNextBatch

}
