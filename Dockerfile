FROM node:20-alpine
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

# Expose API and WebSocket ports
EXPOSE 3000 3001

# Run the startup script
CMD ["sh", "start.sh"]
