import { Construct } from 'constructs';
import path = require('path');

interface QuicksightDataRecord {
  name: string;
  type: string;
  label?: string;
  width?: string;
  isFilterable?: boolean;
}

interface UnaggregateField {
  fieldId: string;
  column: { dataSetIdentifier: string; columnName: string; };
}

interface SelectedFieldOption {
  fieldId: string;
  width: string;
  customLabel: string;
}

interface QuicksightTableDefintionResourceProps {
  readonly sheetName: string;
  readonly tableVisualId: string;
  readonly dataSet: any;
  readonly data: QuicksightDataRecord[];
}

export class QuicksightTableDefintionResource extends Construct {
  public readonly definition: any; // not supported yet by quicksight library

  constructor(scope: Construct, id: string, props: QuicksightTableDefintionResourceProps) {
    super(scope, id);

    const fieldPostfix = 'field';
    const filterGroupIdPostfix = 'filter-group-id';
    const filterControlIdPostfix = 'filter-control-id';
    const filterIdPostfix = 'filter-id';

    const dataSetId = props.dataSet.dataSetId;

    const tableVisual = this.createTableVisual(
      props.tableVisualId,
      props.data,
      dataSetId,
      fieldPostfix,
    );

    const { filterControls, filterGroups } = this.getFilters(
      props.data,
      dataSetId,
      props.sheetName,
      filterGroupIdPostfix,
      filterControlIdPostfix,
      filterIdPostfix,
    )

    const tableLayoutElement = this.createLayoutElement(props.tableVisualId, 0, 32, 1, 16);
    const sheet = this.createTableSheet(
      props.sheetName,
      tableVisual,
      tableLayoutElement,
      filterControls,
    );

    this.definition = this.createQuicksightDefinition(
      props.dataSet.dataSetId,
      props.dataSet.attrArn,
      sheet,
      filterGroups,
    );
  }

  private getFilters = (
    data: QuicksightDataRecord[],
    dataSetId: string,
    sheetName: string,
    filterGroupIdPostfix: string,
    filterControlIdPostfix: string,
    filterIdPostfix: string,
  ) => {
    const filterGroups: any[] = [];
    const filterControls: any[] = [];
    const sheetId = this.getSheetId(sheetName);

    data.forEach((record: QuicksightDataRecord) => {
      {
        if(record.isFilterable) {
          filterGroups.push(
            this.createFilterDropdownGroup(dataSetId, sheetId, record.name, filterGroupIdPostfix, filterIdPostfix)
          );
          filterControls.push(
            this.createFilterDropdownControl(record.name, record.label as string, filterControlIdPostfix, filterIdPostfix)
          );
        }
      }
    });

    return {
      filterGroups,
      filterControls,
    }
  }

  private createQuicksightDefinition = (
    dataSetName: string,
    dataSetArn: string,
    sheet: any,
    filterGroups: any,
  ) => {
    return {
      dataSetIdentifierDeclarations: [
        {
          dataSetArn: dataSetArn,
          identifier: dataSetName,
        },
      ],
      analysisDefaults: {
        defaultNewSheetConfiguration: {
          interactiveLayoutConfiguration: {
            grid: {
              canvasSizeOptions: {
                screenCanvasSizeOptions: {
                  resizeOption: 'FIXED',
                  optimizedViewPortWidth: '1600px',
                },
              },
            },
          },
          sheetContentType: 'INTERACTIVE',
        },
      },
      filterGroups,
      sheets: [
        sheet,
      ],
      contentType: 'INTERACTIVE',
    };
  };

  private createFilterDropdownControl = (
    controlName: string,
    title: string,
    filterControlIdPostfix: string,
    filterIdPostfix: string,
  ) => {
    return {
      dropdown: {
        filterControlId: `${controlName}-${filterControlIdPostfix}`,
        title,
        sourceFilterId: `${controlName}-${filterIdPostfix}`,
        displayOptions: {
          selectAllOptions: {
            visibility: 'VISIBLE',
          },
        },
        type: 'MULTI_SELECT',
      },
    };
  };

  private getSheetId = (sheetName: string) => {
    return sheetName.replace(' ', '-').toLowerCase();
  }

  private createFilterDropdownGroup = (
    dataSetName: string,
    sheetName: string,
    columnName: string,
    filterGroupIdPostfix: string,
    filterIdPostfix: string,
  ) => {
    return {
      filterGroupId: `${columnName}-${filterGroupIdPostfix}`,
      filters: [
        {
          categoryFilter: {
            filterId: `${columnName}-${filterIdPostfix}`,
            column: {
              dataSetIdentifier: dataSetName,
              columnName: columnName,
            },
            configuration: {
              filterListConfiguration: {
                matchOperator: 'CONTAINS',
                selectAllOptions: 'FILTER_ALL_VALUES',
              },
            },
          },
        },
      ],
      scopeConfiguration: {
        selectedSheets: {
          sheetVisualScopingConfigurations: [
            {
              sheetId: this.getSheetId(sheetName),
              scope: 'ALL_VISUALS',
            },
          ],
        },
      },
      status: 'ENABLED',
      crossDataset: 'SINGLE_DATASET',
    };
  };

  private createLayoutElement = (
    visualId: string,
    columnIndex: number,
    columnSpan: number,
    rowIndex: number,
    rowSpan: number,
  ) => {
    return {
      elementId: visualId,
      elementType: 'VISUAL',
      columnIndex,
      columnSpan,
      rowIndex,
      rowSpan,
    }
  }

  private getUnaggregatedFields = (
    data: QuicksightDataRecord[],
    fieldPostfix: string,
    dataSetId: string,
  ) => {
    const unaggregatedFields: UnaggregateField[] = [];

    data.forEach((record: QuicksightDataRecord) => {
      {
        unaggregatedFields.push({
          fieldId: `${record.name}-${fieldPostfix}`,
          column: {
            dataSetIdentifier: dataSetId,
            columnName: record.name,
          }
        });
      }
    });

    return unaggregatedFields;
  }

  private getSelectedFieldOptions = (
    data: QuicksightDataRecord[],
    fieldPostfix: string,
  ) => {
    const selectedFieldOptions: SelectedFieldOption[] = [];

    data.forEach((record: QuicksightDataRecord) => {
      {
        selectedFieldOptions.push({
          fieldId: `${record.name}-${fieldPostfix}`,
          customLabel: record.label as string,
          width: record.width as string,
        });
      }
    });

    return selectedFieldOptions;
  }

  private createTableVisual = (
    tableVisualId: string,
    data: QuicksightDataRecord[],
    dataSetId: string,
    fieldPostfix: string,
) => {
    return {
      tableVisual: {
          visualId: tableVisualId,
          title: {
            visibility: 'HIDDEN',
          },
          subtitle: {
            visibility: 'HIDDEN',
          },
          chartConfiguration: {
            fieldWells: {
              tableUnaggregatedFieldWells: {
                values: this.getUnaggregatedFields(data, fieldPostfix, dataSetId),
              }
            },
            sortConfiguration: {},
            tableOptions: {
              headerStyle: {
                fontConfiguration: {
                  fontWeight: {
                    name: 'BOLD',
                  }
                },
                textWrap: 'WRAP',
              },
              cellStyle: {
                fontConfiguration: {
                  fontStyle: 'NORMAL',
                },
                textWrap: 'WRAP',
                horizontalTextAlignment: 'LEFT',
                verticalTextAlignment: 'TOP',
                height: 154,
              },
              rowAlternateColorOptions: {
                status: 'ENABLED',
              }
            },
            totalOptions: {
              totalsVisibility: 'HIDDEN',
              placement: 'END',
            },
            fieldOptions: {
              selectedFieldOptions: this.getSelectedFieldOptions(data, fieldPostfix),
            }
          },
          actions: []
      }
  }
  }

  public createTableSheet = (
    sheetName: string,
    tableVisual: any,
    tableLayoutElement: any,
    filterControls: any,
  ) => {
    return {
      sheetId: this.getSheetId(sheetName),
      name: sheetName,
      visuals: [tableVisual],
      filterControls,
      layouts: [
        {
          configuration: {
            gridLayout: {
              elements: [tableLayoutElement],
              canvasSizeOptions: {
                screenCanvasSizeOptions: {
                  resizeOption: 'FIXED',
                  optimizedViewPortWidth: '1600px',
                },
              },
            },
          },
        },
      ],
    }
  };
}
