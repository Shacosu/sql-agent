import { Injectable } from '@nestjs/common';
import { runSqlAgent } from './graph';

@Injectable()
export class RagService {
    findAll(): string {
        return "find"
    }

    async ask(question: string) {
        if (!question || question.trim().length === 0) {
            return { ok: false, message: 'Missing query parameter q' };
        }
        const result = await runSqlAgent(question);
        return {
            ok: true,
            question,
            sql: result.sql,
            answer: result.answer,
            rows: result.rows
        };
    }
}
