use anyhow::Result;
use rustdds::{
    with_key::Sample, DomainParticipantBuilder, Keyed, QosPolicyBuilder, TopicDescription,
    TopicKind,
};
use serde::{Deserialize, Serialize};
use tokio;
use tokio_stream::StreamExt;
#[derive(Serialize, Deserialize, Clone, Debug)]
struct SensorConfig {
    sensor_type: String,
    frequency: u32,
    power: u32,
    squelti: u32,
}
impl Keyed for SensorConfig {
    type K = String;
    fn key(&self) -> Self::K {
        self.sensor_type.clone()
    }
}
#[derive(Serialize, Deserialize, Clone, Debug)]
struct SensorBit {
    sensor_type: String,
    bit_on: bool,
}
impl Keyed for SensorBit {
    type K = String;
    fn key(&self) -> Self::K {
        self.sensor_type.clone()
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("Start Subscriber");
    let dp = DomainParticipantBuilder::new(0).build()?;
    let qos = QosPolicyBuilder::new().build();
    let topic_sensor_config = dp.create_topic(
        "SensorConfig".to_string(),
        "this is test topic1".to_string(),
        &qos,
        TopicKind::WithKey,
    )?;
    let topic_sensor_bit = dp.create_topic(
        "SensorBit".to_string(),
        "this is test topic2".to_string(),
        &qos,
        TopicKind::WithKey,
    )?;
    let subscriber = dp.create_subscriber(&qos)?;
    let reader_sensor_config = subscriber
        .create_datareader_cdr::<SensorConfig>(&topic_sensor_config, Some(qos.clone()))?;
    let reader_sensor_bit =
        subscriber.create_datareader_cdr::<SensorBit>(&topic_sensor_bit, Some(qos.clone()))?;
    let mut stream_sensor_config = reader_sensor_config.async_sample_stream();
    let mut stream_sensor_bit = reader_sensor_bit.async_sample_stream();

    loop {
        tokio::select! {
            Some(Ok(Sample::Value(v))) = stream_sensor_config.next() => {
                println!("Receive Topic = {}, message = {:?}", topic_sensor_config.name(), v);
            }
            Some(Ok(Sample::Value(v))) = stream_sensor_bit.next() => {
                println!("Receive Topic = {}, message = {:?}", topic_sensor_bit.name(), v);
            }
            else => { break; }
        }

        //while let Some(Ok(Sample::Value(v))) = stream_sensor_config.next().await {}
    }
    Ok(())
}
