import { Controller, Get, Query, Res } from '@nestjs/common';
import type { Response } from 'express';
import { RagService } from './rag.service';

@Controller('rag')
export class RagController {
  constructor(private readonly ragService: RagService) { }

  @Get('ask')
  async ask(@Query('q') q: string, @Query('format') format: string, @Res() res: Response) {
    const data = await this.ragService.ask(q ?? '');
    if (format === 'sql') {
      res.setHeader('Content-Type', 'text/plain; charset=utf-8');
      return res.send(data.sql ?? '');
    }
    if (format === 'md') {
      res.setHeader('Content-Type', 'text/markdown; charset=utf-8');
      const body = '```sql\n' + (data.sql ?? '') + '\n```\n';
      return res.send(body);
    }
    if (format === 'sqldownload') {
      res.setHeader('Content-Type', 'text/plain; charset=utf-8');
      res.setHeader('Content-Disposition', 'attachment; filename="query.sql"');
      return res.send(data.sql ?? '');
    }
    return res.json(data);
  }
}
