import { ach } from '@st-achievements/database';
import { InferSelectModel, sql, SQL } from 'drizzle-orm';
import { PgColumn } from 'drizzle-orm/pg-core';

import { AchievementInputDto } from './achievement-input.dto.js';

interface QueryOperatorOptions {
  readonly achievement: Readonly<InferSelectModel<typeof ach.achievement>>;
  readonly achievementWorkoutTypes: Readonly<
    InferSelectModel<typeof ach.achievementWorkoutType>
  >;
  readonly input: Readonly<AchievementInputDto>;
}

export interface QueryOperator {
  init(options: QueryOperatorOptions): this;
  condition(query: Readonly<Query>): boolean;
  execute(query: Query): Query;
}

interface QuerySelect {
  value: SQL<number>;
  by?: SQL | PgColumn;
}

interface Query {
  where: SQL[];
  select: QuerySelect;
}

export class QueryProcessor {
  constructor(private readonly options: QueryOperatorOptions) {}

  private where: SQL[] = [];
  private select: QuerySelect = {
    value: sql`0`,
  };

  pipe(...processors: QueryOperator[]): QueryProcessor {
    for (const processor of processors) {
      if (!processor.init(this.options).condition(this.get())) {
        continue;
      }
      const { where, select } = processor.execute(this.get());
      this.select = select;
      this.where = where;
    }
    return this;
  }

  get(): Query {
    return {
      select: this.select,
      where: this.where,
    };
  }
}
