import { HttpStatus } from '@nestjs/common';
import { exception } from '@st-api/core';

export const PLATINUM_NOT_FOUND = exception({
  errorCode: 'USR-AP-0001',
  status: HttpStatus.INTERNAL_SERVER_ERROR,
  message: 'Platinum achievement not found',
  error: 'Platinum achievement not found',
});

export const PERIOD_NOT_FOUND = exception({
  errorCode: 'USR-AP-0002',
  status: HttpStatus.BAD_REQUEST,
  message: 'Period not found',
  error: 'Period not found',
});

export const INVALID_REDIS_CREDENTIALS = exception({
  errorCode: 'USR-AP-0003',
  status: HttpStatus.INTERNAL_SERVER_ERROR,
  message: 'Invalid Redis Credentials',
});

export const WORKOUT_NOT_FOUND = exception({
  errorCode: 'USR-AP-0004',
  status: HttpStatus.BAD_REQUEST,
  message: 'Workout not found',
  error: 'Workout not found',
});
