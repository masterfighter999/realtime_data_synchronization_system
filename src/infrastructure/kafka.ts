import { Kafka, SASLOptions } from 'kafkajs';
import fs from 'fs';
import { config } from '../shared/config';
import { logger } from '../shared/logger';

/**
 * Resolves SSL certificates from environment strings or local file paths.
 */
function getSSLConfig() {
  const { kafkaCaCert, kafkaAccessCert, kafkaAccessKey } = config;

  if (!kafkaCaCert) {
    // If no CA certificate is provided, check if broker URI is SSL or fallback to no SSL
    return config.kafkaBrokers[0].startsWith('localhost') ? false : true;
  }

  const resolvePem = (value: string): string => {
    // If it is an inline PEM string, replace literal \n with newlines
    if (value.startsWith('-----BEGIN')) {
      return value.replace(/\\n/g, '\n');
    }
    // Otherwise, assume it is a file path
    try {
      if (fs.existsSync(value)) {
        return fs.readFileSync(value, 'utf-8');
      }
    } catch (error: any) {
      logger.warn({ path: value, err: error.message }, 'Could not read certificate path, passing as-is');
    }
    return value;
  };

  try {
    const ca = resolvePem(kafkaCaCert);
    const cert = kafkaAccessCert ? resolvePem(kafkaAccessCert) : undefined;
    const key = kafkaAccessKey ? resolvePem(kafkaAccessKey) : undefined;

    return {
      rejectUnauthorized: false, // Fail-open to support custom domain mappings and development testing
      ca: [ca],
      cert,
      key,
    };
  } catch (error: any) {
    logger.error({ err: error.message }, 'Failed to parse Kafka SSL certificates');
    return true;
  }
}

/**
 * Resolves SASL options (Aiven SCRAM-SHA-256 or PLAIN).
 */
function getSASLConfig(): SASLOptions | undefined {
  const { kafkaSaslUsername, kafkaSaslPassword } = config;
  if (kafkaSaslUsername && kafkaSaslPassword) {
    return {
      mechanism: 'scram-sha-256', // Default for Aiven Kafka SASL
      username: kafkaSaslUsername,
      password: kafkaSaslPassword,
    };
  }
  return undefined;
}

// Instantiate Kafka Client
export const kafka = new Kafka({
  clientId: config.kafkaClientId,
  brokers: config.kafkaBrokers,
  ssl: getSSLConfig(),
  sasl: getSASLConfig(),
  connectionTimeout: 10000,
  authenticationTimeout: 10000,
});

export const producer = kafka.producer();

// Create Consumer group specifically for our WebSocket Gateway instances
export const consumer = kafka.consumer({ groupId: 'rtds-websocket-gateways' });

/**
 * Connects Kafka Producer and handles connection failures gracefully.
 */
export async function connectProducer() {
  try {
    await producer.connect();
    logger.info('Kafka Producer connected successfully');
  } catch (error: any) {
    logger.error({ err: error.message }, 'Failed to connect Kafka Producer');
  }
}
