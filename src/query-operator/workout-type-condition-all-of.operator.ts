import { usr } from '@st-achievements/database';

import { QueryOperator } from '../query/query-operator.js';
import { Query, QueryResult } from '../query/query.type.js';

export class WorkoutTypeConditionAllOfOperator extends QueryOperator {
  condition(): boolean {
    return this.options.achievement.workoutTypeCondition === 'allOf';
  }

  execute(query: Query): Query {
    query.select.by = usr.workout.workoutTypeId;
    return query;
  }

  override isComplete(values: QueryResult[]): boolean {
    return values.length >= this.options.achievementWorkoutTypes.length;
  }
}
