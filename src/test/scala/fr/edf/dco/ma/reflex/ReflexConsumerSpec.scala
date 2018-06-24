package fr.edf.dco.ma.reflex

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.duration._
import scala.util.Random

class ReflexConsumerSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender
  with WordSpecLike with Matchers with BeforeAndAfterAll {
  def this() = this(ActorSystem("MySpec"))


//  /** ***************************************
//    * HELPER METHODS
//    */
//
//  val kafkaServer = new KafkaServer()
//
//  def randomString: String = Random.alphanumeric.take(5).mkString("")
//
//
//  val config: Config = ConfigFactory.parseString(
//    s"""
//       | bootstrap.servers = "localhost:${kafkaServer.kafkaPort}",
//       | auto.offset.reset = "earliest",
//       | group.id = "$randomString"
//        """.stripMargin
//  )
//
//  override def beforeAll() = {
//
//    kafkaServer.startup()
//  }
//
//
//  override def afterAll() = {
//    TestKit.shutdownActorSystem(system)
//    producer.close()
//    kafkaServer.close()
//  }
//
//  val keySerializer = new StringSerializer()
//  val valueSerializer = new StringSerializer()
//  val keyDeserializer = new StringDeserializer()
//  val valueDeserializer = new StringDeserializer()
//
//
//  val producer =
//    KafkaProducer(KafkaProducer.Conf(keySerializer, valueSerializer, bootstrapServers = s"localhost:${kafkaServer.kafkaPort}"))
//
//  def submitMsg(times: Int, topic: String, msg: ReflexMessage) = {
//    for (i <- 1 to times) {
//      producer.send(KafkaProducerRecord(topic, randomString, msg.text))
//      producer.flush()
//    }
//  }
//
//
//  /** **********************************************
//    * Les tests
//    */
//
//  "A display actor" must {
//    "display all filtered messages" in {
//      def filterFunction(reflexMessage: ReflexMessage): Boolean = true
//
//      val displayActor = system.actorOf(DisplayActor.props, "DisplayTest")
//      val filterActor = system.actorOf(FilterActor.props(filterFunction, "test", displayActor, config), "FilterTest")
//      val tester = TestProbe()
//      tester.watch(filterActor)
//      tester.watch(displayActor)
//      submitMsg(3, "test", ReflexMessage("Hello World !"))
//
//      Thread.sleep(5000)
//      filterActor ! StopWorking
//      tester.expectTerminated(filterActor, 10 seconds)
//    }
//  }


}

