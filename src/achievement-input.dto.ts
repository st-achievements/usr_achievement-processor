import { z } from 'zod';

const DatetimeSchema = z
  .string()
  .trim()
  .datetime()
  .transform((value) => new Date(value));

export const AchievementInputDto = z.object({
  achievementIds: z.number().array(),
  workoutDate: DatetimeSchema,
  userId: z.number(),
  periodId: z.number(),
  workoutId: z.number(),
});

export type AchievementInputDto = z.infer<typeof AchievementInputDto>;

export type AchievementInputSingleDto = Omit<
  AchievementInputDto,
  'achievementIds'
> & { achievementId: number };
