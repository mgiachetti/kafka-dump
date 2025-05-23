use config::{Config, ConfigError, Environment, File};
use serde::{Deserialize, Serialize};
use std::env;
use std::fmt::{self, Display};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct KafkaAuthConfig {
    pub username: String,
    pub password: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct KafkaConfig {
    pub brokers: String,
    pub topics: String,
    pub groupid: String,
    pub auth: KafkaAuthConfig,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub enum AppEnv {
    #[serde(rename = "development")]
    Development,
    #[serde(rename = "production")]
    Production,
}

impl Display for AppEnv {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        let value: &str = &(serde_json::to_string(self).unwrap());
        return formatter.write_str(&value[1..(value.len() - 1)]);
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct S3Config {
    pub bucketprefix: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Settings {
    pub env: AppEnv,
    pub kafka: KafkaConfig,
    pub s3: S3Config,
}

impl Settings {
    pub fn load() -> Result<Settings, ConfigError> {
        let mut s = Config::new();

        s.merge(File::with_name("config/defaults"))?;

        if let Ok(app_env) = env::var("APP_ENV") {
            s.set("env", app_env).expect("Error setting env var");
        }

        let env: String = s
            .get("env")
            .expect(r#"Cannot load config. "env" nor APP_ENV not present"#);

        let env_config: String = format!("config/{}", env);
        if std::path::Path::new(&env_config).exists() {
            s.merge(File::with_name(&env_config))?;
        }
        s.merge(Environment::with_prefix("app").separator("_"))?;

        return s.try_into();
    }
}

#[test]
fn test_load_default_settings() {
    let _ = Settings::load();
}
