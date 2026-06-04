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
    let cleaned = value.trim();
    // Strip surrounding double or single quotes if present
    if (cleaned.startsWith('"') && cleaned.endsWith('"')) {
      cleaned = cleaned.substring(1, cleaned.length - 1);
    } else if (cleaned.startsWith("'") && cleaned.endsWith("'")) {
      cleaned = cleaned.substring(1, cleaned.length - 1);
    }
    cleaned = cleaned.trim();

    let pemContent = cleaned;

    // If it contains the PEM header, treat it as inline certificate content
    if (cleaned.includes('-----BEGIN')) {
      pemContent = cleaned.replace(/\\n/g, '\n');
    } else {
      // Otherwise, assume it is a local file path
      try {
        if (fs.existsSync(cleaned)) {
          pemContent = fs.readFileSync(cleaned, 'utf-8');
        }
      } catch (error: any) {
        logger.warn({ path: cleaned, err: error.message }, 'Could not read certificate path, passing as-is');
      }
    }

    // Strip any Windows carriage return (\r) characters to prevent OpenSSL parsing errors on Linux
    return pemContent.replace(/\r/g, '');
  };

  try {
    const ca = resolvePem(kafkaCaCert);
    const cert = kafkaAccessCert ? resolvePem(kafkaAccessCert) : undefined;
    const key = kafkaAccessKey ? resolvePem(kafkaAccessKey) : undefined;

    logger.info({
      caLength: ca ? ca.length : 0,
      certLength: cert ? cert.length : 0,
      keyLength: key ? key.length : 0,
      caPreview: ca ? ca.substring(0, 30) + '...' + ca.substring(ca.length - 30) : '',
      certPreview: cert ? cert.substring(0, 30) + '...' + cert.substring(cert.length - 30) : '',
      keyPreview: key ? key.substring(0, 30) + '...' + key.substring(key.length - 30) : '',
    }, 'Resolved Kafka certificates');

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
