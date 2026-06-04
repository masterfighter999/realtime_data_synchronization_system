import jwt from 'jsonwebtoken';
import { Request, Response, NextFunction } from 'express';
import { config } from '../shared/config';
import { logger } from '../shared/logger';

export interface UserPayload {
  username: string;
  role: 'admin' | 'customer';
}

// Add user context typing to Express Request
declare global {
  namespace Express {
    interface Request {
      user?: UserPayload;
    }
  }
}

export function signToken(payload: UserPayload): string {
  return jwt.sign(payload, config.jwtSecret, { expiresIn: '24h' });
}

export function verifyToken(token: string): UserPayload {
  return jwt.verify(token, config.jwtSecret) as UserPayload;
}

/**
 * Middleware protecting routes with JWT validation.
 */
export function authMiddleware(req: Request, res: Response, next: NextFunction) {
  const authHeader = req.headers.authorization;
  if (!authHeader || !authHeader.startsWith('Bearer ')) {
    return res.status(401).json({ error: 'Unauthorized: Missing token' });
  }

  const token = authHeader.split(' ')[1];
  try {
    const decoded = verifyToken(token);
    req.user = decoded;
    next();
  } catch (error: any) {
    logger.warn({ err: error.message }, 'JWT authentication failed');
    return res.status(401).json({ error: 'Unauthorized: Invalid or expired token' });
  }
}

/**
 * Middleware restricting access to administrator accounts.
 */
export function requireAdmin(req: Request, res: Response, next: NextFunction) {
  if (!req.user || req.user.role !== 'admin') {
    return res.status(403).json({ error: 'Forbidden: Admin access required' });
  }
  next();
}
