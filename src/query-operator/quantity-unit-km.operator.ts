import { usr } from '@st-achievements/database';
import { sum } from 'drizzle-orm';

import { QuantityUnitEnum } from '../quantity-unit.enum.js';
import { QueryOperator } from '../query/query-operator.js';
import { Query } from '../query/query.type.js';

export class QuantityUnitKMOperator extends QueryOperator {
  condition(): boolean {
    return this.options.achievement.quantityUnitId === QuantityUnitEnum.KM;
  }

  execute(query: Query): Query {
    query.select.value = sum(usr.workout.distance).mapWith(Number);
    return query;
  }
}
