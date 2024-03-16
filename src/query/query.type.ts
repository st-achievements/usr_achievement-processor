import { ach, cfg } from '@st-achievements/database';
import { InferSelectModel, SQL } from 'drizzle-orm';
import { PgColumn } from 'drizzle-orm/pg-core';
import { Simplify } from 'type-fest';

import { AchievementInputSingleDto } from '../achievement-input.dto.js';

export interface QueryOptions {
  readonly achievement: Readonly<InferSelectModel<typeof ach.achievement>>;
  readonly achievementWorkoutTypes: Readonly<
    InferSelectModel<typeof ach.achievementWorkoutType>
  >[];
  readonly period: Readonly<InferSelectModel<typeof cfg.period>>;
  readonly input: Readonly<AchievementInputSingleDto>;
}

export interface QueryIsComplete {
  (values: QueryResult[]): boolean;
}

export interface QueryGetProgressQuantity {
  (values: QueryResult[]): number;
}

export interface QuerySelect {
  value: SQL<number>;
  by?: SQL | PgColumn;
}

export interface QueryResult {
  value: number;
  by?: unknown;
}

export interface QueryProcessed {
  isComplete: QueryIsComplete;
  getProgressQuantity: QueryGetProgressQuantity;
  select: Simplify<QuerySelect>;
  where: SQL[];
}

export interface Query {
  where: SQL[];
  select: QuerySelect;
}
