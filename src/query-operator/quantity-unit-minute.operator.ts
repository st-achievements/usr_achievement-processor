import { usr } from '@st-achievements/database';
import { sum } from 'drizzle-orm';

import { QuantityUnitEnum } from '../quantity-unit.enum.js';
import { QueryOperator } from '../query/query-operator.js';
import { Query } from '../query/query.type.js';

export class QuantityUnitMinuteOperator extends QueryOperator {
  condition(): boolean {
    return this.options.achievement.quantityUnitId === QuantityUnitEnum.Minute;
  }

  execute(query: Query): Query {
    query.select.value = sum(usr.workout.duration).mapWith(Number);
    return query;
  }
}
