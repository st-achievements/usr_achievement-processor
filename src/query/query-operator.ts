import { Query, QueryOptions, QueryResult } from './query.type.js';

// eslint-disable-next-line @typescript-eslint/no-unsafe-declaration-merging
export interface QueryOperator {
  isComplete?(values: QueryResult[]): boolean;
  getProgressQuantity?(values: QueryResult[]): number;
}

// eslint-disable-next-line @typescript-eslint/no-unsafe-declaration-merging
export abstract class QueryOperator {
  public constructor(protected readonly options: QueryOptions) {}

  abstract condition(query: Readonly<Query>): boolean;
  abstract execute(query: Query): Query;
}
