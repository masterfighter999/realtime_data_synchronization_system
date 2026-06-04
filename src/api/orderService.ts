import { Order } from '@prisma/client';
import { prisma } from '../infrastructure/db';
import { runWithDbBreaker } from '../infrastructure/circuitBreaker';
import { invalidateOrderCache, setCachedOrder } from '../cache/orderCache';
import { v4 as uuidv4 } from 'uuid';
import { logger } from '../shared/logger';

export interface CreateOrderInput {
  customer_name: string;
  product_name: string;
  status: string;
}

export interface UpdateOrderInput {
  customer_name?: string;
  product_name?: string;
  status?: string;
}

export class OrderService {
  /**
   * Creates a new order and appends an outbox event atomically.
   */
  static async createOrder(input: CreateOrderInput): Promise<Order> {
    return runWithDbBreaker(async () => {
      const eventId = uuidv4();
      
      const order = await prisma.$transaction(async (tx) => {
        const newOrder = await tx.order.create({
          data: {
            customer_name: input.customer_name,
            product_name: input.product_name,
            status: input.status,
          },
        });

        await tx.outboxEvent.create({
          data: {
            event_id: eventId,
            aggregate_type: 'Order',
            aggregate_id: newOrder.id.toString(),
            event_type: 'ORDER_CREATED',
            payload: {
              id: newOrder.id,
              customer_name: newOrder.customer_name,
              product_name: newOrder.product_name,
              status: newOrder.status,
              updated_at: newOrder.updated_at.toISOString(),
            },
            event_version: 1,
          },
        });

        return newOrder;
      });

      logger.info({ id: order.id }, 'Order created and outbox event queued');
      
      // Update cache
      await setCachedOrder(order);
      return order;
    });
  }

  /**
   * Updates an existing order and appends an outbox event atomically.
   */
  static async updateOrder(id: number, input: UpdateOrderInput): Promise<Order> {
    return runWithDbBreaker(async () => {
      const eventId = uuidv4();

      const order = await prisma.$transaction(async (tx) => {
        const updatedOrder = await tx.order.update({
          where: { id },
          data: input,
        });

        await tx.outboxEvent.create({
          data: {
            event_id: eventId,
            aggregate_type: 'Order',
            aggregate_id: updatedOrder.id.toString(),
            event_type: 'ORDER_UPDATED',
            payload: {
              id: updatedOrder.id,
              customer_name: updatedOrder.customer_name,
              product_name: updatedOrder.product_name,
              status: updatedOrder.status,
              updated_at: updatedOrder.updated_at.toISOString(),
            },
            event_version: 1,
          },
        });

        return updatedOrder;
      });

      logger.info({ id }, 'Order updated and outbox event queued');

      // Update cache
      await invalidateOrderCache(id);
      await setCachedOrder(order);
      return order;
    });
  }

  /**
   * Deletes an order and appends an outbox event atomically.
   */
  static async deleteOrder(id: number): Promise<Order> {
    return runWithDbBreaker(async () => {
      const eventId = uuidv4();

      const order = await prisma.$transaction(async (tx) => {
        const deletedOrder = await tx.order.delete({
          where: { id },
        });

        await tx.outboxEvent.create({
          data: {
            event_id: eventId,
            aggregate_type: 'Order',
            aggregate_id: deletedOrder.id.toString(),
            event_type: 'ORDER_DELETED',
            payload: {
              id: deletedOrder.id,
              customer_name: deletedOrder.customer_name,
              product_name: deletedOrder.product_name,
              status: deletedOrder.status,
              updated_at: deletedOrder.updated_at.toISOString(),
            },
            event_version: 1,
          },
        });

        return deletedOrder;
      });

      logger.info({ id }, 'Order deleted and outbox event queued');

      // Invalidate cache
      await invalidateOrderCache(id);
      return order;
    });
  }
}
