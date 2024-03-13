import { usr } from '@st-achievements/database';
import { eq } from 'drizzle-orm';

import { QueryOperator } from '../query/query-operator.js';
import { Query, QueryResult } from '../query/query.type.js';

export class InitialOperator extends QueryOperator {
  condition(): boolean {
    return true;
  }

  execute(query: Query): Query {
    query.where.push(
      eq(usr.workout.userId, this.options.input.userId),
      eq(usr.workout.active, true),
      eq(usr.workout.periodId, this.options.input.periodId),
    );
    return query;
  }

  override isComplete([value]: QueryResult[]): boolean {
    return (
      (value?.value ?? Number.NEGATIVE_INFINITY) >=
      this.options.achievement.quantityNeeded
    );
  }

  override getProgressQuantity([value]: QueryResult[]): number {
    return value?.value ?? 0;
  }
}
