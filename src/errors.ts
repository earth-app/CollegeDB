/**
 * @fileoverview Custom error class for CollegeDB operations
 *
 * This module provides the CollegeDBError class that extends the native Error class
 * to provide more specific error information for CollegeDB operations. This allows
 * for better error handling and debugging throughout the application.
 *
 * @example
 * ```typescript
 * import { CollegeDBError } from './errors';
 *
 * throw new CollegeDBError('Failed to allocate shard', 'SHARD_ALLOCATION_ERROR');
 * ```
 *
 * @author Gregory Mitchell
 * @since 1.0.2
 */

/**
 * Custom error class for CollegeDB operations
 *
 * Extends the native Error class to provide more specific error information
 * for CollegeDB operations. Includes an optional error code for better
 * error categorization and handling.
 *
 * @example
 * ```typescript
 * try {
 *   await getShardForKey('invalid-key');
 * } catch (error) {
 *   if (error instanceof CollegeDBError) {
 *     console.error(`CollegeDB Error (${error.code}): ${error.message}`);
 *   }
 * }
 * ```
 */
export class CollegeDBError extends Error {
	/**
	 * Optional error code for categorizing different types of errors
	 */
	public readonly code?: string;

	/**
	 * Creates a new CollegeDBError instance
	 * @param message - Error message describing what went wrong
	 * @param code - Optional error code for categorization
	 * @example
	 * ```typescript
	 * throw new CollegeDBError('Shard not found', 'SHARD_NOT_FOUND');
	 * ```
	 */
	constructor(message: string, code?: string) {
		super(message);
		this.name = 'CollegeDBError';
		this.code = code;

		// Trims the constructor frames off the stack. V8 provides this in Node, Bun
		// and workerd, but it is not part of the language, so it is read off a
		// narrowed view of the constructor rather than assumed to exist.
		const v8Error = Error as ErrorConstructor & {
			captureStackTrace?: (target: object, constructorOpt?: Function) => void;
		};
		v8Error.captureStackTrace?.(this, CollegeDBError);
	}
}
