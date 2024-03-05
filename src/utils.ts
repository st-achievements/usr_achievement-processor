export function arraySumBy<T>(
  array: T[],
  callback: (item: T, index: number, array: T[]) => number,
): number {
  return array.reduce(
    (acc, item, index, innerArray) => acc + callback(item, index, innerArray),
    0,
  );
}
