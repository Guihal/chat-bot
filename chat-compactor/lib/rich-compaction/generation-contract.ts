import type { DirectContent } from "../shared/isolated-pi-types.js";

/** Pure semantic contract; model selection and execution stay outside it. */
export interface GenerationContract<TInput, TOutput> {
  readonly id: string;
  readonly systemPrompt: string;
  encodeInput(input: TInput): readonly DirectContent[];
  parseFinal(text: string): TOutput;
  validate(output: TOutput): void;
}
