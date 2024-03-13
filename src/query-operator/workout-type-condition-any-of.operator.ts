import { usr } from '@st-achievements/database';
import { inArray } from 'drizzle-orm';

import { QueryOperator } from '../query/query-operator.js';
import { Query } from '../query/query.type.js';

export class WorkoutTypeConditionAnyOfOperator extends QueryOperator {
  condition(): boolean {
    return this.options.achievement.workoutTypeCondition === 'anyOf';
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
    return query;
  }
}
