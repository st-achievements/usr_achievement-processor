import { usr } from '@st-achievements/database';
import { inArray } from 'drizzle-orm';

import { QueryOperator } from '../query/query-operator.js';
import { Query, QueryResult } from '../query/query.type.js';

export class WorkoutTypeConditionExclusiveAnyOfOperator extends QueryOperator {
  condition(): boolean {
    return this.options.achievement.workoutTypeCondition === 'exclusiveAnyOf';
  }

  execute(query: Query): Query {
    query.where.push(
      inArray(
        usr.workout.workoutTypeId,
        this.options.achievementWorkoutTypes.map(
          (workoutType) => workoutType.workoutTypeId,
        ),
      ),
    );
    query.select.by = usr.workout.workoutTypeId;
    return query;
  }

  override isComplete(values: QueryResult[]): boolean {
    return values.length >= this.options.achievement.quantityNeeded;
  }
}
