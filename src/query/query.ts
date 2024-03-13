import { Logger } from '@st-api/firebase';
import { sql, SQL } from 'drizzle-orm';
import { Class } from 'type-fest';

import { QueryOperator } from './query-operator.js';
import {
  QueryGetProgressQuantity,
  QueryIsComplete,
  QueryOptions,
  QueryProcessed,
  QuerySelect,
} from './query.type.js';

export class QueryProcessor {
  private where: SQL[] = [];
  private select: QuerySelect = {
    value: sql`0`,
  };
  private isComplete: QueryIsComplete = () => false;
  private getProgressQuantity: QueryGetProgressQuantity = () => 0;
  private executed = false;
  private readonly processors: Class<QueryOperator, [QueryOptions]>[] = [];

  private readonly logger = Logger.create(this);

  pipe(...processors: Class<QueryOperator, [QueryOptions]>[]): this {
    this.processors.push(...processors);
    return this;
  }

  get(): QueryProcessed {
    return {
      select: this.select,
      where: this.where,
      isComplete: this.isComplete,
      getProgressQuantity: this.getProgressQuantity,
    };
  }

  execute(options: QueryOptions): this {
    if (this.executed) {
      return this;
    }
    const seenProcessor = new Set<Class<QueryOperator>>();
    const usedProcessors: string[] = [];
    for (const processor of this.processors) {
      if (seenProcessor.has(processor)) {
        continue;
      }
      seenProcessor.add(processor);
      const instance = new processor(options);
      const queryOptions = this.get();
      if (!instance.condition(queryOptions)) {
        continue;
      }
      usedProcessors.push(processor.name);
      const { where, select } = instance.execute(queryOptions);
      if (instance.isComplete) {
        this.isComplete = instance.isComplete.bind(instance);
      }
      if (instance.getProgressQuantity) {
        this.getProgressQuantity = instance.getProgressQuantity.bind(instance);
      }
      this.select = select;
      this.where = where;
    }
    this.logger.info(`used processors`, { usedProcessors });
    this.executed = true;
    return this;
  }
}
