FROM node:20-slim
RUN apt-get update -y && apt-get install -y openssl ca-certificates && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install dependencies (both production and development)
COPY package*.json ./
RUN npm ci

# Copy application source
COPY . .

# Generate Prisma client and compile TypeScript
RUN npx prisma generate
RUN npm run build

# Make startup script executable
RUN chmod +x start.sh

# Expose API and WebSocket port (shared)
EXPOSE 3000

# Run the startup script
CMD ["sh", "start.sh"]
