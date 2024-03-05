import { HttpStatus } from '@nestjs/common';
import { exception } from '@st-api/core';

export const PLATINUM_NOT_FOUND = exception({
  errorCode: 'USR-AP-0001',
  status: HttpStatus.INTERNAL_SERVER_ERROR,
  message: 'Platinum achievement not found',
  error: 'Platinum achievement not found',
});
