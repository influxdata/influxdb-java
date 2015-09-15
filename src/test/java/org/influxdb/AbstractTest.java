package org.influxdb;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.core.DockerClientBuilder;
import com.github.dockerjava.core.DockerClientConfig;
import org.influxdb.dto.Pong;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

public abstract class AbstractTest
{
   private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(AbstractTest.class);

   protected InfluxDB influxDB;
   private DockerClient dockerClient;
   private CreateContainerResponse container;
   /**
    * Create a influxDB connection before all tests start.
    *
    * @throws InterruptedException
    * @throws IOException
    */
   @BeforeClass
   public void setUp() throws InterruptedException, IOException {
      // Disable logging for the DockerClient.
      Logger.getLogger("com.sun.jersey").setLevel(Level.OFF);
      DockerClientConfig config = DockerClientConfig
              .createDefaultConfigBuilder()
              .withVersion("1.16")
              .withUri("tcp://localhost:4243")
              .withUsername("roott")
              .withPassword("root")
              .build();
      dockerClient = DockerClientBuilder.getInstance(config).build();
      // this.dockerClient.pullImageCmd("majst01/influxdb-java");

      // ExposedPort tcp8086 = ExposedPort.tcp(8086);
      //
      // Ports portBindings = new Ports();
      // portBindings.bind(tcp8086, Ports.Binding(8086));
      // this.container = this.dockerClient.createContainerCmd("influxdb:0.9.0-rc7").exec();
      // this.dockerClient.startContainerCmd(this.container.getId()).withPortBindings(portBindings).exec();
      //
      // InspectContainerResponse inspectContainerResponse =
      // this.dockerClient.inspectContainerCmd(
      // this.container.getId()).exec();
      //
      // InputStream containerLogsStream = this.dockerClient
      // .logContainerCmd(this.container.getId())
      // .withStdErr()
      // .withStdOut()
      // .exec();

      // String ip = inspectContainerResponse.getNetworkSettings().getIpAddress();
      String ip = "127.0.0.1";
      influxDB = InfluxDBFactory.connect("http://" + ip + ":8086", "root", "root");
      boolean influxDBstarted = false;
      do {
         Pong response;
         try {
            response = influxDB.ping();
            LOGGER.info("Response {}", response);
            if (!response.getVersion().equalsIgnoreCase("unknown")) {
               influxDBstarted = true;
            }
         } catch (Exception e) {
            // NOOP intentional
            LOGGER.error(e.getMessage(), e);
         }
         Thread.sleep(100L);
      } while (!influxDBstarted);
      influxDB.setLogLevel(InfluxDB.LogLevel.FULL);
      LOGGER.info("##################################################################################");
      LOGGER.info("#  Connected to InfluxDB Version: " + influxDB.version() + " #");
      LOGGER.info("##################################################################################");
   }

   /**
    * Ensure all Databases created get dropped afterwards.
    */
   @AfterClass
   public void tearDown() {
      LOGGER.info("Kill the Docker container");
      // this.dockerClient.killContainerCmd(this.container.getId()).exec();
   }
}
