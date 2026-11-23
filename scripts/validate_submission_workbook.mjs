import { FileBlob, SpreadsheetFile } from '@oai/artifact-tool';

const path = 'E:/INCUBIX/submission/Assessment4_CandidateReview_Draft.xlsx';
const workbook = await SpreadsheetFile.importXlsx(await FileBlob.load(path));
workbook.recalculate();
const sheets = workbook.worksheets.items.map((sheet) => sheet.name);
const expected = ['2_Must_haves', '3_Levels', '4_Integration', '5_Metrics', '6_Hard_cases', '7_Declaration'];
const missing = expected.filter((name) => !sheets.includes(name));
if (missing.length) throw new Error(`Missing workbook sheets: ${missing.join(', ')}`);
const errors = await workbook.inspect({
  kind: 'match',
  searchTerm: '#REF!|#DIV/0!|#VALUE!|#NAME\\?|#N/A|#NUM!|#NULL!|#SPILL!|#CALC!',
  options: { useRegex: true, maxResults: 300 },
  summary: 'submission workbook formula error scan',
});
const errorLines = errors.ndjson.split('\n').filter((line) => line.includes('"kind":"match"'));
if (errorLines.length) throw new Error(`Formula errors found:\n${errorLines.join('\n')}`);
for (const name of expected) {
  const preview = await workbook.render({ sheetName: name, autoCrop: 'all', scale: 1, format: 'png' });
  if ((await preview.arrayBuffer()).byteLength === 0) throw new Error(`Could not render ${name}`);
}
console.log(`Validated ${expected.length} required workbook sheets with no formula errors.`);
