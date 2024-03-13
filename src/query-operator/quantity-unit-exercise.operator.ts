import { count } from 'drizzle-orm';

import { QuantityUnitEnum } from '../quantity-unit.enum.js';
import { QueryOperator } from '../query/query-operator.js';
import { Query } from '../query/query.type.js';

export class QuantityUnitExerciseOperator extends QueryOperator {
  condition(): boolean {
    return (
      this.options.achievement.quantityUnitId === QuantityUnitEnum.Exercise
    );
  }

  execute(query: Query): Query {
    query.select.value = count();
    return query;
  }
}
