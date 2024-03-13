import { usr } from '@st-achievements/database';
import { eq } from 'drizzle-orm';

import { QueryOperator } from '../query/query-operator.js';
import { Query } from '../query/query.type.js';

export class PeriodConditionSingleOperator extends QueryOperator {
  condition(): boolean {
    return this.options.achievement.periodCondition === 'singleSession';
  }

  execute(query: Query): Query {
    query.where.push(eq(usr.workout.id, this.options.input.workoutId));
    return query;
  }
}
