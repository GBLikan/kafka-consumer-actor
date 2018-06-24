package fr.edf.dco.ma.reflex

import java.util.Properties

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import com.typesafe.config.{Config, ConfigFactory}
import fr.edf.dco.ma.reflex.ReflexKafkaConsumerActor.{PleasePoll, StopWorking}
import info.batey.kafka.unit.KafkaUnit
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, OffsetResetStrategy}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.duration._
import scala.util.Random

class ReflexConsumerSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender
  with WordSpecLike with Matchers with BeforeAndAfterAll {
  def this() = this(ActorSystem("MySpec"))


  /** ***************************************
    * HELPER METHODS
    */

  val kafkaServer = new KafkaUnit(2181, 9092)

  def randomString: String = Random.alphanumeric.take(5).mkString("")


  val config: Config = ConfigFactory.parseString(
    s"""
       | bootstrap.servers = "localhost:${kafkaServer.getKafkaConnect}",
       | auto.offset.reset = "earliest",
       | group.id = "$randomString"
        """.stripMargin
  )

  override def beforeAll() = {
    kafkaServer.startup()
  }


  override def afterAll() = {
    TestKit.shutdownActorSystem(system)
    producer.close()
    kafkaServer.shutdown()
  }

  val keySerializer = new StringSerializer()
  val valueSerializer = new StringSerializer()
  val keyDeserializer = new StringDeserializer()
  val valueDeserializer = new StringDeserializer()

  //Props du consumer
  var props = new Properties()
  props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ConsumerConfig.GROUP_ID_CONFIG, randomString)
  props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
  props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
  props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
  props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
  props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.EARLIEST.toString.toLowerCase)

  //Props du producer
  var prodProps = new Properties()
  prodProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  prodProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  prodProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")

  val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](prodProps)

  def submitMsg(times: Int, topic: String, msg: String) = {
    for (i <- 1 to times) {
      producer.send(new ProducerRecord[String, String]("test", i.toString, msg))
      producer.flush()
    }
  }


  /** **********************************************
    * Les tests
    */

  "A display actor" must {
    "display all filtered messages" in {
      def filterFunction(cr: ConsumerRecord[String, String]): Boolean = true

      val displayer: ActorRef = system.actorOf(ReflexKafkaDisplayActor.props, "displayTestActor")
      val filterer: ActorRef = system.actorOf(ReflexKafkaFilterActor.props(filterFunction, displayer), name = "filterTestActor")
      val consumer: ActorRef = system.actorOf(ReflexKafkaConsumerActor.props("test", props, filterer), name = "consumerTestActor")
      val tester = TestProbe()
      tester.watch(displayer)
      tester.watch(filterer)
      tester.watch(consumer)

      consumer ! PleasePoll

      submitMsg(3, "test", "Hello World !")

      consumer ! PleasePoll

      consumer ! StopWorking
      tester.expectTerminated(consumer, 10 seconds)
    }
  }


}

